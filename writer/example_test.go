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
