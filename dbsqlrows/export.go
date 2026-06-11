package dbsqlrows

import (
	"database/sql"
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

var (
	// ErrNilRows reports that a dbsqlrows entry point was called with a nil *sql.Rows
	// (for example [WriteRows], [RunRows], [RunRowsAtData], [WriteRowsAtData], or
	// [ReadMetadataAndAdvanceToData]).
	ErrNilRows = errors.New("nil sql.Rows")
	// ErrNilWriter reports that an export entry point was called with a nil [GCVStreamWriter]
	// (for example [WriteRows] or [WriteRowsAtData]).
	ErrNilWriter = errors.New("nil GCV stream writer")
	// ErrNilMetadata reports that a data-phase entry point was called with nil metadata
	// (for example [WriteRowsAtData] or [RunRowsAtData]).
	ErrNilMetadata = errors.New("nil result set metadata")
	// ErrMissingMetadataRow reports that the iterator produced no metadata
	// pseudo-row when WriteRows expected one.
	ErrMissingMetadataRow = errors.New("missing result set metadata row")
	// ErrMissingDataResultSet reports that NextResultSet did not advance to the
	// data rows result set after the metadata pseudo-row.
	ErrMissingDataResultSet = errors.New("missing data rows result set after metadata")
	// ErrMissingStatsRow reports that the stats result set had no stats pseudo-row.
	ErrMissingStatsRow = errors.New("missing result set stats row")
	// ErrMissingStatsResultSet reports that NextResultSet did not advance to the
	// stats result set when [SQLRowsConfig.ReadResultSetStats] was requested.
	// This typically means the driver was not opened with ReturnResultSetStats
	// enabled (see the precondition on [SQLRowsConfig.ReadResultSetStats]).
	ErrMissingStatsResultSet = errors.New("missing result set stats result set")
)

// GCVStreamWriter is the subset of [github.com/apstndb/spanvalue/writer] types
// that dbsqlrows drives. Built-in writers also implement PrepareRowType or
// Prepare for metadata registration; [SQLRowsHooksFromGCVWriter] calls those when
// present after reading the metadata pseudo-row.
type GCVStreamWriter interface {
	WriteGCVs([]spanner.GenericColumnValue) error
	Flush() error
}

// SQLRowsConfig configures a SQL rows streaming run.
type SQLRowsConfig struct {
	// ReadResultSetStats, when true, advances past data rows to read the stats
	// pseudo-row into [SQLRowsResult.Stats]. For [WriteRowsAtData] this field is
	// consulted directly (default false). For [WriteRows] the same field applies.
	//
	// Precondition: the driver must produce a stats pseudo result set (for
	// go-sql-spanner, open rows with ReturnResultSetStats: true). When no stats
	// result set follows the data rows, the run fails with
	// [ErrMissingStatsResultSet]. Note that in a multi-statement batch with
	// driver stats disabled, NextResultSet can instead land on the next
	// statement's metadata result set; the resulting scan error is reported only
	// after that pseudo-row has been consumed, so the batch cursor position is
	// not recoverable.
	ReadResultSetStats bool
}

// WithReadResultSetStats returns a copy of cfg with ReadResultSetStats set.
func (cfg SQLRowsConfig) WithReadResultSetStats(read bool) SQLRowsConfig {
	cfg.ReadResultSetStats = read
	return cfg
}

// SQLRowsResult holds metadata and stats surfaced from driver pseudo result sets,
// analogous to [writer.RowIteratorResult] for native iterators.
// On error paths after metadata is known, Metadata and RowsRead reflect progress
// at the abort point (same partial-result contract as writer row-iterator helpers).
// Stats is also populated on the abort path when the stats pseudo-row was
// already read but the trailing NextResultSet advance failed.
//
// RowsRead counts data rows for which [SQLRowsHooks].WriteDataRow returned nil.
// It stays zero when WriteDataRow is nil (rows may still be drained), matching
// [github.com/apstndb/spanvalue/writer.RowIteratorResult] RowsRead semantics.
type SQLRowsResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    *sppb.ResultSetStats
	RowsRead int
}

// WriteRows streams an open *sql.Rows positioned at the metadata pseudo-row
// into w. The caller must open rows with a driver that returns proto-decoded
// GCV columns and a leading metadata pseudo result set (see README). The caller
// retains ownership of rows and must Close it and check [sql.Rows.Err] when
// appropriate.
//
// On success WriteRows calls [GCVStreamWriter.Flush] and returns its error
// explicitly (do not defer Flush at the call site). Data rows are scanned into
// []spanner.GenericColumnValue.
//
// When ReadResultSetStats is false (the default), rows remain on the data
// result set after export so the caller can advance to stats separately.
//
// For custom sinks (for example ASCII table rendering), use [RunRows] with
// [SQLRowsHooks] instead.
func WriteRows(rows *sql.Rows, w GCVStreamWriter, cfg SQLRowsConfig) (*SQLRowsResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return RunRows(rows, SQLRowsHooksFromGCVWriter(w), cfg)
}

// WriteRowsAtData streams rows already positioned on the data result set into w.
// metadata must be non-nil (typically from [ReadMetadataAndAdvanceToData] or an
// earlier statement in a batch). The writer is prepared from metadata when it
// implements PrepareRowType or Prepare.
//
// Stats are not consumed unless cfg.ReadResultSetStats is true, so callers can
// render first and read stats from rows afterward (spannersh execution summary).
//
// For custom sinks, use [RunRowsAtData] with [SQLRowsHooks].
func WriteRowsAtData(
	rows *sql.Rows,
	metadata *sppb.ResultSetMetadata,
	w GCVStreamWriter,
	cfg SQLRowsConfig,
) (*SQLRowsResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if metadata == nil {
		return nil, ErrNilMetadata
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return RunRowsAtData(rows, metadata, SQLRowsHooksFromGCVWriter(w), cfg)
}

type sqlRowsRunConfig struct {
	metadata              *sppb.ResultSetMetadata
	readMetadataPseudoRow bool
	readResultSetStats    bool
}

func runRows(fac rowsFacade, hooks SQLRowsHooks, run sqlRowsRunConfig) (*SQLRowsResult, error) {
	result := &SQLRowsResult{Metadata: run.metadata}
	abort := func(err error) (*SQLRowsResult, error) {
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
			return abort(ErrMissingDataResultSet)
		}
	} else if run.metadata == nil {
		return nil, ErrNilMetadata
	}

	if err := callPrepareMetadata(hooks, run.metadata); err != nil {
		return abort(err)
	}

	if err := processDataRows(fac, hooks, result); err != nil {
		return abort(err)
	}
	if err := fac.err(); err != nil {
		return abort(err)
	}

	if run.readResultSetStats {
		stats, err := readResultSetStats(fac)
		// Populate Stats even on the abort path: when the stats pseudo-row was
		// read but the trailing NextResultSet advance failed, the caller still
		// gets the completed statement's stats (partial-result contract).
		result.Stats = stats
		if err != nil {
			return abort(err)
		}
	}

	return finishRun(result, hooks)
}

// runRowsWithGCVWriter is the test seam for runRows with a GCV writer.
func runRowsWithGCVWriter(fac rowsFacade, w GCVStreamWriter, run sqlRowsRunConfig) (*SQLRowsResult, error) {
	return runRows(fac, SQLRowsHooksFromGCVWriter(w), run)
}

func callPrepareMetadata(hooks SQLRowsHooks, md *sppb.ResultSetMetadata) error {
	if hooks.PrepareMetadata == nil {
		return nil
	}
	return hooks.PrepareMetadata(md)
}

func processDataRows(fac rowsFacade, hooks SQLRowsHooks, result *SQLRowsResult) error {
	if hooks.WriteDataRow == nil {
		// Drain without decoding. RowsRead stays zero so the count keeps the
		// same meaning as writer.RowIteratorResult.RowsRead (rows written by
		// the per-row callback), not rows merely consumed.
		for fac.next() {
		}
		return nil
	}
	n, err := fac.columnCount()
	if err != nil {
		return err
	}
	// gcvs and dest are allocated once per result set; scan overwrites gcvs in place
	// before each WriteDataRow call. Hooks must not retain the slice after returning.
	gcvs, dest := gcvScanTargets(n)
	for fac.next() {
		if err := fac.scan(dest...); err != nil {
			return err
		}
		if err := hooks.WriteDataRow(gcvs); err != nil {
			return err
		}
		result.RowsRead++
	}
	return nil
}

// readResultSetStats advances to the stats result set, scans the stats
// pseudo-row, and advances past it for multi-statement batches. When the scan
// succeeded but the trailing NextResultSet advance fails, the scanned stats are
// returned alongside the error so the caller can surface them at the abort
// point.
func readResultSetStats(fac rowsFacade) (*sppb.ResultSetStats, error) {
	if !fac.nextResultSet() {
		if err := fac.err(); err != nil {
			return nil, err
		}
		// No stats result set after the data rows: the driver was not opened
		// with stats enabled. Fail loudly instead of silently returning nil
		// stats (see SQLRowsConfig.ReadResultSetStats).
		return nil, ErrMissingStatsResultSet
	}
	if !fac.next() {
		if err := fac.err(); err != nil {
			return nil, err
		}
		return nil, ErrMissingStatsRow
	}
	var stats *sppb.ResultSetStats
	if err := fac.scan(&stats); err != nil {
		return nil, err
	}
	if !fac.nextResultSet() {
		if err := fac.err(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func finishRun(result *SQLRowsResult, hooks SQLRowsHooks) (*SQLRowsResult, error) {
	if hooks.Finish != nil {
		if err := hooks.Finish(result); err != nil {
			return result, err
		}
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
