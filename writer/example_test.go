package writer_test

import (
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue/writer"
)

// ExampleWriteRowSeq streams a client-side (virtual) result set — rows that
// do not come from a *spanner.RowIterator — through a CSV writer, with
// explicit metadata supplying the header.
func ExampleWriteRowSeq() {
	names := []string{"name", "value"}
	row1, err := spanner.NewRow(names, []any{"AUTOCOMMIT", "TRUE"})
	if err != nil {
		fmt.Println(err)
		return
	}
	row2, err := spanner.NewRow(names, []any{"READONLY", "FALSE"})
	if err != nil {
		fmt.Println(err)
		return
	}

	md := &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{
		{Name: "name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
		{Name: "value", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
	}}}

	w, err := writer.NewCSVWriter(os.Stdout)
	if err != nil {
		fmt.Println(err)
		return
	}
	if _, err := writer.WriteRowSeq(md, writer.RowSeq(row1, row2), w); err != nil {
		fmt.Println(err)
		return
	}
	// Output:
	// name,value
	// AUTOCOMMIT,TRUE
	// READONLY,FALSE
}

// ExampleRunRowSeqWithStats processes a retained result, including its DML
// count, without constructing a Spanner client or RowIterator.
func ExampleRunRowSeqWithStats() {
	row, err := spanner.NewRow([]string{"id"}, []any{int64(7)})
	if err != nil {
		fmt.Println(err)
		return
	}
	var stats writer.RowIteratorStats
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(row, nil) {
			return
		}
		stats.RowCount = 1
	}
	hooks := writer.NewRowIteratorHooks().
		WithWriteRow(func(row *spanner.Row) error {
			var id int64
			if err := row.Column(0, &id); err != nil {
				return err
			}
			fmt.Println("id:", id)
			return nil
		}).
		WithFinish(func(result *writer.RowIteratorResult) error {
			fmt.Println("affected:", result.Stats.RowCount)
			return nil
		})
	if _, err := writer.RunRowSeqWithStats(nil, rows, func() writer.RowIteratorStats { return stats }, hooks); err != nil {
		fmt.Println(err)
	}
	// Output:
	// id: 7
	// affected: 1
}
