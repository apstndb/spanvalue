package writer

import (
	"bytes"
	"testing"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestFormatSQLInsertFragment_matchesWriter_scaffold(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name"}
	values := []spanner.GenericColumnValue{gcvctor.Int64Value(42), gcvctor.StringValue("Alice")}
	formatter := spanvalue.LiteralFormatConfig()

	var writerOut bytes.Buffer
	w := mustNewSQLInsertWriter(t, &writerOut, "users")
	if err := w.WriteValues(columnNames, values); err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}

	formatted, err := spanvalue.FormatRowColumns(formatter, columnNames, values)
	if err != nil {
		t.Fatalf("FormatRowColumns() error = %v", err)
	}
	prefix, err := FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", columnNames)
	if err != nil {
		t.Fatalf("FormatSQLInsertPrefix() error = %v", err)
	}
	got := FormatSQLInsertStatement(prefix, formatted)

	if diff := cmp.Diff(writerOut.String(), got); diff != "" {
		t.Fatalf("fragment assembly mismatch (-writer +fragment):\n%s", diff)
	}
}

func TestFormatSQLInsertFragment_batchedAssembly(t *testing.T) {
	t.Parallel()
	t.Skip("WIP #79: multi-row fragment helpers (VALUES joiner, Flush suffix) not designed yet")

	// Intended sketch: custom batchers assemble
	//   prefix + tuple1 + ",\n  (" + tuple2 + ");\n"
	// matching SQLInsertWriter batch output from TestSQLInsertWriterBatching.
}

func TestFormatSQLInsertFragment_insertOrKinds(t *testing.T) {
	t.Parallel()
	t.Skip("WIP #79: document whether INSERT OR IGNORE prefixes belong in FormatSQLInsertPrefix only or need sibling helpers")
}
