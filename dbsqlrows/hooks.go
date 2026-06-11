package dbsqlrows

import (
	"database/sql"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// SQLRowsHooks drives [RunRows] and [RunRowsAtData]. Nil function fields are skipped.
//
// An empty hooks value (from [NewSQLRowsHooks]) still advances past data rows
// while WriteDataRow is nil (no per-row decode), but [SQLRowsResult.RowsRead]
// stays zero for drained rows, matching [writer.RowIteratorResult] RowsRead
// semantics. Use that to drain rows before reading stats (for example EXPLAIN
// with [SQLRowsConfig.ReadResultSetStats]).
//
// PrepareMetadata runs once after metadata is known and before data rows are scanned.
// WriteDataRow runs per data row when set. The []spanner.GenericColumnValue argument
// is reused across calls: valid only for the duration of WriteDataRow; copy or
// format synchronously before returning if the sink retains row data. Finish runs only after all rows and
// optional stats consumption succeed; it is not called when PrepareMetadata or
// WriteDataRow returns an error. The returned [SQLRowsResult] still carries
// Metadata and RowsRead at the abort point (same partial-result contract as
// [writer.RowIteratorHooks] and [writer.RunRowIterator]).
type SQLRowsHooks struct {
	PrepareMetadata func(*sppb.ResultSetMetadata) error
	WriteDataRow    func([]spanner.GenericColumnValue) error
	Finish          func(*SQLRowsResult) error
}

// NewSQLRowsHooks returns an empty hooks value for custom decoration or
// [SQLRowsHooksFromGCVWriter].
func NewSQLRowsHooks() SQLRowsHooks {
	return SQLRowsHooks{}
}

// WithPrepareMetadata sets PrepareMetadata and returns h.
func (h SQLRowsHooks) WithPrepareMetadata(fn func(*sppb.ResultSetMetadata) error) SQLRowsHooks {
	h.PrepareMetadata = fn
	return h
}

// WithWriteDataRow sets WriteDataRow and returns h. The slice passed to fn is reused
// on each row; do not retain it after fn returns.
func (h SQLRowsHooks) WithWriteDataRow(fn func([]spanner.GenericColumnValue) error) SQLRowsHooks {
	h.WriteDataRow = fn
	return h
}

// WithFinish sets Finish and returns h.
func (h SQLRowsHooks) WithFinish(fn func(*SQLRowsResult) error) SQLRowsHooks {
	h.Finish = fn
	return h
}

// SQLRowsHooksFromGCVWriter returns hooks that register metadata via
// [GCVStreamWriter] PrepareRowType or Prepare when implemented, write each row
// with [GCVStreamWriter.WriteGCVs], and call [GCVStreamWriter.Flush] in Finish.
// Finish (and thus Flush) runs only after all rows and optional stats consumption succeed;
// it is skipped when PrepareMetadata, WriteDataRow, or the stats phase returns an error.
// A nil writer returns empty hooks.
func SQLRowsHooksFromGCVWriter(w GCVStreamWriter) SQLRowsHooks {
	if w == nil {
		return NewSQLRowsHooks()
	}
	return NewSQLRowsHooks().
		WithPrepareMetadata(func(md *sppb.ResultSetMetadata) error {
			return prepareWriterMetadata(w, md)
		}).
		WithWriteDataRow(w.WriteGCVs).
		WithFinish(func(*SQLRowsResult) error {
			return w.Flush()
		})
}

// RunRows streams an open *sql.Rows positioned at the metadata pseudo-row using
// hooks. See [WriteRows] for driver conventions, ownership, and stats behavior.
func RunRows(rows *sql.Rows, hooks SQLRowsHooks, cfg SQLRowsConfig) (*SQLRowsResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	return runRows(sqlRowsFacade{rows}, hooks, sqlRowsRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    cfg.ReadResultSetStats,
	})
}

// RunRowsAtData streams rows already positioned on the data result set using hooks.
// metadata must be non-nil. See [WriteRowsAtData] for stats and partial-result
// semantics.
func RunRowsAtData(
	rows *sql.Rows,
	metadata *sppb.ResultSetMetadata,
	hooks SQLRowsHooks,
	cfg SQLRowsConfig,
) (*SQLRowsResult, error) {
	if rows == nil {
		return nil, ErrNilRows
	}
	if metadata == nil {
		return nil, ErrNilMetadata
	}
	return runRows(sqlRowsFacade{rows}, hooks, sqlRowsRunConfig{
		metadata:              metadata,
		readMetadataPseudoRow: false,
		readResultSetStats:    cfg.ReadResultSetStats,
	})
}
