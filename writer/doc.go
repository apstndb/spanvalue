// Package writer streams Spanner query results to delimited text, JSONL, or SQL INSERT
// using [github.com/apstndb/spanvalue] formatters.
//
// Main types: [DelimitedWriter], [JSONLWriter], [SQLInsertWriter], and the [Writer] /
// [FlushWriter] interfaces. Register column schema with [WithColumnNames], [WithRowType],
// or [WithMetadata] (or [DelimitedWriter.PrepareRowType] / [DelimitedWriter.PrepareColumnNames]
// after construction). [DelimitedWriter] buffers through encoding/csv—call [Flusher.Flush]
// after the final row.
//
// Extended documentation, RowIterator recipes, and module-split notes:
// https://github.com/apstndb/spanvalue/blob/main/writer/README.md
//
// # RowIterator
//
// [WriteRowIterator] targets built-in [RowIteratorWriter] implementations
// ([DelimitedWriter], [JSONLWriter], [SQLInsertWriter]) via [RowIteratorHooksFromWriter].
// [RunRowIterator] is the extension point for other sinks: supply [RowIteratorHooks] built with
// [NewRowIteratorHooks] and the With* setters, or decorate with [WithRowOrdinal],
// [ObserveWriteRow], and [AfterEachSuccessfulWriteRow]. Both helpers own the iterator they
// receive: they consume it, call [*cloud.google.com/go/spanner.RowIterator.Stop], and return
// [RowIteratorResult] (metadata, stats, [RowIteratorResult.RowsRead]). Prefer passing a
// newly created iterator directly (for example txn.Query(ctx, stmt)); do not defer Stop at
// the call site. Use the returned result for post-run metadata and stats.
//
// For manual [*cloud.google.com/go/spanner.RowIterator.Next] loops, bind the iterator,
// defer Stop, register [RowIteratorWriter.PrepareRowType] after the first Next when results
// may be empty, and read metadata or stats only after consuming to [google.golang.org/api/iterator.Done]; propagate
// [Flusher.Flush] errors (do not defer Flush).
//
// # Direct writers vs hooks
//
// Prefer [WriteRowIterator] when the destination is a package writer. Prefer [RunRowIterator]
// when formatting or export logic should stay outside spanvalue. Prefer [Writer.WriteRow] or
// [*DelimitedWriter.WriteGCVs] without RowIterator when the app already owns iteration (for
// example [database/sql] scans—see the root README go-sql-spanner section).
//
// [DelimitedGCVExportOptions] and [JSONLGCVExportOptions] group metadata, formatter,
// and unnamed-field namer options for GCV slice export.
//
// # Quoted delimited text vs raw tab-separated
//
// [DelimitedWriter] uses encoding/csv rules (RFC 4180-style). [NewDelimitedWriter] with
// delimiter '\t' produces quoted TSV, not a raw join of formatted strings. Legacy raw TAB
// export can implement [Writer] or [RowIteratorWriter] and join columns with '\t'.
//
// # Column names and registered schema
//
// [*DelimitedWriter.WriteGCVs] and [*DelimitedWriter.WriteStructValues] require prior
// name/type registration. [Writer.WriteRow] and [*DelimitedWriter.WriteValues] supply names
// per call. Writers distinguish missing schema from registered zero-column schema; see
// [ErrMissingColumnNames]. [WithMetadata] from a [cloud.google.com/go/spanner.RowIterator]
// before the first Next registers an empty schema—use [RowIteratorWriter.PrepareRowType] after
// the first Next or [WriteRowIterator] instead.
//
// # SQL INSERT
//
// [NewSQLInsertWriter] accepts [WithSQLInsertKind], [WithSQLDialect], and [WithSQLBatchSize].
// After any write error from [SQLInsertWriter], discard the writer. [*SQLInsertWriter.Flush]
// closes a partial batch when batching.
package writer
