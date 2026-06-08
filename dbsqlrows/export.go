package dbsqlrows

import (
	"context"
	"database/sql"
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	spannerdriver "github.com/googleapis/go-sql-spanner"

	"github.com/apstndb/spanvalue"
)

var (
	// ErrNilRows reports that ExportRows was called with a nil *sql.Rows.
	ErrNilRows = errors.New("nil sql.Rows")
	// ErrNilWriter reports that ExportRows was called with a nil GCVStreamWriter.
	ErrNilWriter = errors.New("nil GCV stream writer")
	// ErrMissingMetadataRow reports that ReturnResultSetMetadata was enabled but
	// the iterator produced no metadata pseudo-row.
	ErrMissingMetadataRow = errors.New("missing result set metadata row")
)

// DefaultExecOptions is the recommended go-sql-spanner configuration for
// [ExportRows] and [QueryExport]: proto-decoded rows plus a leading metadata
// pseudo result set ([spannerdriver.ExecOptions.ReturnResultSetMetadata]).
var DefaultExecOptions = spannerdriver.ExecOptions{
	DecodeOption:            spannerdriver.DecodeOptionProto,
	ReturnResultSetMetadata: true,
}

// GCVStreamWriter is the subset of [github.com/apstndb/spanvalue/writer] types
// that dbsqlrows drives. Built-in writers also implement PrepareRowType or
// Prepare for metadata registration; dbsqlrows calls those when present after
// reading the metadata pseudo-row.
type GCVStreamWriter interface {
	WriteGCVs([]spanner.GenericColumnValue) error
	Flush() error
}

// ExportConfig configures a go-sql-spanner export run.
type ExportConfig struct {
	// ExecOptions is passed to [database/sql.DB.QueryContext] by [QueryExport].
	// When zero, [DefaultExecOptions] is used.
	ExecOptions spannerdriver.ExecOptions
	// Formatter and Namer are reserved for constructor helpers that build writers
	// after metadata is known (see README). ExportRows does not read them when the
	// caller supplies a pre-built writer.
	Formatter *spanvalue.FormatConfig
	Namer     spanvalue.UnnamedFieldNamer
}

// ExportResult holds metadata and stats surfaced from go-sql-spanner pseudo
// result sets, analogous to [writer.RowIteratorResult] for native iterators.
type ExportResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    *sppb.ResultSetStats
	RowsRead int
}

// ExportRows streams an open *sql.Rows (proto-decoded, metadata-first result
// sets when [DefaultExecOptions] apply) into w. The caller retains ownership of
// rows and must Close it and check [sql.Rows.Err] when appropriate.
//
// On success ExportRows calls [GCVStreamWriter.Flush] and returns its error
// explicitly (do not defer Flush at the call site). Metadata is read from the
// first pseudo result set when ExecOptions request it; data rows are scanned into
// []spanner.GenericColumnValue per [spannerdriver.DecodeOptionProto].
func ExportRows(rows *sql.Rows, w GCVStreamWriter, cfg ExportConfig) (*ExportResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	opts := effectiveExecOptions(cfg)
	return exportRows(sqlRowsFacade{rows}, w, opts)
}

// QueryExport runs db.QueryContext with cfg.ExecOptions and exports the result.
// It closes rows before returning.
func QueryExport(
	ctx context.Context,
	db *sql.DB,
	query string,
	args []any,
	w GCVStreamWriter,
	cfg ExportConfig,
) (*ExportResult, error) {
	if db == nil {
		return nil, errors.New("nil *sql.DB")
	}
	opts := effectiveExecOptions(cfg)
	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, opts)
	queryArgs = append(queryArgs, args...)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return ExportRows(rows, w, cfg)
}

func effectiveExecOptions(cfg ExportConfig) spannerdriver.ExecOptions {
	if cfg.ExecOptions == (spannerdriver.ExecOptions{}) {
		return DefaultExecOptions
	}
	return cfg.ExecOptions
}

func exportRows(fac rowsFacade, w GCVStreamWriter, opts spannerdriver.ExecOptions) (*ExportResult, error) {
	result := &ExportResult{}

	if opts.ReturnResultSetMetadata {
		if !fac.next() {
			if err := fac.err(); err != nil {
				return nil, err
			}
			return nil, ErrMissingMetadataRow
		}
		var md *sppb.ResultSetMetadata
		if err := fac.scan(&md); err != nil {
			return nil, err
		}
		result.Metadata = md
		if err := prepareWriterMetadata(w, md); err != nil {
			return nil, err
		}
		if !fac.nextResultSet() {
			if err := fac.err(); err != nil {
				return nil, err
			}
			return finishExport(result, w)
		}
	}

	for fac.next() {
		n, err := fac.columnCount()
		if err != nil {
			return nil, err
		}
		gcvs, dest := gcvScanTargets(n)
		if err := fac.scan(dest...); err != nil {
			return nil, err
		}
		if err := w.WriteGCVs(gcvs); err != nil {
			return result, err
		}
		result.RowsRead++
	}
	if err := fac.err(); err != nil {
		return result, err
	}

	if opts.ReturnResultSetStats {
		if fac.nextResultSet() && fac.next() {
			var stats *sppb.ResultSetStats
			if err := fac.scan(&stats); err != nil {
				return result, err
			}
			result.Stats = stats
		}
	}

	return finishExport(result, w)
}

func finishExport(result *ExportResult, w GCVStreamWriter) (*ExportResult, error) {
	if err := w.Flush(); err != nil {
		return result, err
	}
	return result, nil
}

func prepareWriterMetadata(w GCVStreamWriter, md *sppb.ResultSetMetadata) error {
	if md == nil {
		return nil
	}
	if p, ok := w.(interface {
		PrepareRowType(*sppb.StructType) error
	}); ok {
		return p.PrepareRowType(md.GetRowType())
	}
	if p, ok := w.(interface {
		Prepare(*sppb.ResultSetMetadata) error
	}); ok {
		return p.Prepare(md)
	}
	return nil
}

func gcvScanTargets(n int) ([]spanner.GenericColumnValue, []any) {
	gcvs := make([]spanner.GenericColumnValue, n)
	dest := make([]any, n)
	for i := range gcvs {
		dest[i] = &gcvs[i]
	}
	return gcvs, dest
}
