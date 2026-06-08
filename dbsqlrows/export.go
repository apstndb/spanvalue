package dbsqlrows

import (
	"database/sql"
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

var (
	// ErrNilRows reports that ExportRows was called with a nil *sql.Rows.
	ErrNilRows = errors.New("nil sql.Rows")
	// ErrNilWriter reports that ExportRows was called with a nil GCVStreamWriter.
	ErrNilWriter = errors.New("nil GCV stream writer")
	// ErrNilMetadata reports that ExportRowsAtData was called with nil metadata.
	ErrNilMetadata = errors.New("nil result set metadata")
	// ErrMissingMetadataRow reports that the iterator produced no metadata
	// pseudo-row when ExportRows expected one.
	ErrMissingMetadataRow = errors.New("missing result set metadata row")
	// ErrMissingDataResultSet reports that NextResultSet did not advance to the
	// data rows result set after the metadata pseudo-row.
	ErrMissingDataResultSet = errors.New("missing data rows result set after metadata")
	// ErrMissingStatsRow reports that the stats result set had no stats pseudo-row.
	ErrMissingStatsRow = errors.New("missing result set stats row")
)

// GCVStreamWriter is the subset of [github.com/apstndb/spanvalue/writer] types
// that dbsqlrows drives. Built-in writers also implement PrepareRowType or
// Prepare for metadata registration; dbsqlrows calls those when present after
// reading the metadata pseudo-row.
type GCVStreamWriter interface {
	WriteGCVs([]spanner.GenericColumnValue) error
	Flush() error
}

// ExportConfig configures an export run.
type ExportConfig struct {
	// ReadResultSetStats, when true, advances past data rows to read the stats
	// pseudo-row into [ExportResult.Stats]. For [ExportRowsAtData] this field is
	// consulted directly (default false). For [ExportRows] the same field applies.
	ReadResultSetStats bool
}

// WithReadResultSetStats returns a copy of cfg with ReadResultSetStats set.
func (cfg ExportConfig) WithReadResultSetStats(read bool) ExportConfig {
	cfg.ReadResultSetStats = read
	return cfg
}

// ExportResult holds metadata and stats surfaced from driver pseudo result sets,
// analogous to [writer.RowIteratorResult] for native iterators.
// On error paths after metadata is known, Metadata and RowsRead reflect progress
// at the abort point (same partial-result contract as writer row-iterator helpers).
type ExportResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    *sppb.ResultSetStats
	RowsRead int
}

// ExportRows streams an open *sql.Rows positioned at the metadata pseudo-row
// into w. The caller must open rows with a driver that returns proto-decoded
// GCV columns and a leading metadata pseudo result set (see README). The caller
// retains ownership of rows and must Close it and check [sql.Rows.Err] when
// appropriate.
//
// On success ExportRows calls [GCVStreamWriter.Flush] and returns its error
// explicitly (do not defer Flush at the call site). Data rows are scanned into
// []spanner.GenericColumnValue.
//
// When ReadResultSetStats is false (the default), rows remain on the data
// result set after export so the caller can advance to stats separately.
func ExportRows(rows *sql.Rows, w GCVStreamWriter, cfg ExportConfig) (*ExportResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return exportRows(sqlRowsFacade{rows}, w, exportRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    cfg.ReadResultSetStats,
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
				return abort(err)
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
	n, err := fac.columnCount()
	if err != nil {
		return err
	}
	gcvs, dest := gcvScanTargets(n)
	for fac.next() {
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
		return nil, ErrMissingStatsRow
	}
	var stats *sppb.ResultSetStats
	if err := fac.scan(&stats); err != nil {
		return nil, err
	}
	_ = fac.nextResultSet()
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
