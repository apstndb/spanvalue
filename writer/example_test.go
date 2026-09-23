package writer_test

import (
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanvalue/writer"
)

func ExampleFormatSQLInsertPrefix() {
	prefix, err := writer.FormatSQLInsertPrefix(
		writer.SQLInsert,
		databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
		"users",
		[]string{"id", "name"},
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	tuple := func(id, name string) string {
		return writer.FormatSQLInsertValuesTuple([]string{id, name})
	}
	// The caller owns the batch size and joins tuples. This is not a writer API.
	fmt.Print(prefix + "\n  " + tuple("1", `"a"`) + ",\n  " + tuple("2", `"b"`) + ";\n")
	fmt.Print(prefix + "\n  " + tuple("3", `"c"`) + ";\n")
	// Output:
	// INSERT INTO `users` (`id`, `name`) VALUES
	//   (1, "a"),
	//   (2, "b");
	// INSERT INTO `users` (`id`, `name`) VALUES
	//   (3, "c");
}

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
