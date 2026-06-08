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
	// ErrNilMetadata reports that ExportRowsAtData was called with nil metadata.
	ErrNilMetadata = errors.New("nil result set metadata")
	// ErrMissingMetadataRow reports that ReturnResultSetMetadata was enabled but
	// the iterator produced no metadata pseudo-row.
	ErrMissingMetadataRow = errors.New("missing result set metadata row")
)

// DefaultExecOptions is the recommended go-sql-spanner configuration for
// [ExportRows] and [QueryExport]: proto-decoded rows plus a leading metadata
// pseudo result set ([spannerdriver.ExecOptions.ReturnResultSetMetadata]).
// ReturnResultSetStats is false so callers can read stats after export (for
// example spannersh execution summaries).
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
	// When zero, [DefaultExecOptions] is used. [ExportRows] also uses
	// ExecOptions.ReturnResultSetStats to decide whether to consume the trailing
	// stats pseudo-row; leave false when the caller reads stats after export.
	ExecOptions spannerdriver.ExecOptions
	// Formatter and Namer are reserved for constructor helpers that build writers
	// after metadata is known (see README). ExportRows does not read them when the
	// caller supplies a pre-built writer.
	Formatter *spanvalue.FormatConfig
	Namer     spanvalue.UnnamedFieldNamer
	// ReadResultSetStats, when true, advances past data rows to read the stats
	// pseudo-row into [ExportResult.Stats]. For [ExportRowsAtData] this field is
	// consulted directly (default false). For [ExportRows] it overrides
	// ExecOptions.ReturnResultSetStats when set together with a non-zero
	// ExecOptions via [ExportConfig.WithReadResultSetStats].
	ReadResultSetStats bool
}

// WithReadResultSetStats returns a copy of cfg with ReadResultSetStats set.
func (cfg ExportConfig) WithReadResultSetStats(read bool) ExportConfig {
	cfg.ReadResultSetStats = read
	return cfg
}

// ExportResult holds metadata and stats surfaced from go-sql-spanner pseudo
// result sets, analogous to [writer.RowIteratorResult] for native iterators.
// On error paths after metadata is known, Metadata and RowsRead reflect progress
// at the abort point (same partial-result contract as writer row-iterator helpers).
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
//
// When ExecOptions.ReturnResultSetStats is false (the default), rows remain on the
// data result set after export so the caller can advance to stats separately.
func ExportRows(rows *sql.Rows, w GCVStreamWriter, cfg ExportConfig) (*ExportResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	opts := effectiveExecOptions(cfg)
	return exportRows(sqlRowsFacade{rows}, w, exportRunConfig{
		readMetadataPseudoRow: opts.ReturnResultSetMetadata,
		readResultSetStats:    consumeResultSetStats(cfg, opts),
	})
}

// ExportRowsAtData streams rows already positioned on the data result set into w.
// metadata must be non-nil (typically from [ReadMetadataAndAdvanceToData] or an
// earlier statement in a batch). The writer is prepared from metadata when it
// implements PrepareRowType or Prepare.
//
// Stats are not consumed unless cfg.ReadResultSetStats is true, so callers can
// render first and read stats from rows afterward (spannersh execution summary).
func ExportRowsAtData(
	rows *sql.Rows,
	metadata *sppb.ResultSetMetadata,
	w GCVStreamWriter,
	cfg ExportConfig,
) (*ExportResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if metadata == nil {
		return nil, ErrNilMetadata
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return exportRows(sqlRowsFacade{rows}, w, exportRunConfig{
		metadata:              metadata,
		readMetadataPseudoRow: false,
		readResultSetStats:    cfg.ReadResultSetStats,
	})
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

func consumeResultSetStats(cfg ExportConfig, opts spannerdriver.ExecOptions) bool {
	if cfg.ReadResultSetStats {
		return true
	}
	if cfg.ExecOptions != (spannerdriver.ExecOptions{}) {
		return opts.ReturnResultSetStats
	}
	return false
}

type exportRunConfig struct {
	metadata              *sppb.ResultSetMetadata
	readMetadataPseudoRow bool
	readResultSetStats    bool
}

func exportRows(fac rowsFacade, w GCVStreamWriter, run exportRunConfig) (*ExportResult, error) {
	result := &ExportResult{Metadata: run.metadata}
	abort := func(err error) (*ExportResult, error) {
		return result, err
	}

	if run.readMetadataPseudoRow {
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
		run.metadata = md
		if !fac.nextResultSet() {
			if err := fac.err(); err != nil {
				return nil, err
			}
			if err := prepareWriterMetadata(w, md); err != nil {
				return abort(err)
			}
			return finishExport(result, w)
		}
	} else if run.metadata == nil {
		return nil, ErrNilMetadata
	}

	if err := prepareWriterMetadata(w, run.metadata); err != nil {
		return abort(err)
	}

	if err := exportDataRows(fac, w, result); err != nil {
		return abort(err)
	}
	if err := fac.err(); err != nil {
		return abort(err)
	}

	if run.readResultSetStats {
		stats, err := readResultSetStats(fac)
		if err != nil {
			return abort(err)
		}
		result.Stats = stats
	}

	return finishExport(result, w)
}

func exportDataRows(fac rowsFacade, w GCVStreamWriter, result *ExportResult) error {
	for fac.next() {
		n, err := fac.columnCount()
		if err != nil {
			return err
		}
		gcvs, dest := gcvScanTargets(n)
		if err := fac.scan(dest...); err != nil {
			return err
		}
		if err := w.WriteGCVs(gcvs); err != nil {
			return err
		}
		result.RowsRead++
	}
	return nil
}

func readResultSetStats(fac rowsFacade) (*sppb.ResultSetStats, error) {
	if !fac.nextResultSet() {
		if err := fac.err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if !fac.next() {
		return nil, errors.New("expected result set stats row")
	}
	var stats *sppb.ResultSetStats
	if err := fac.scan(&stats); err != nil {
		return nil, err
	}
	return stats, nil
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
