package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

func TestFormatSQLInsertFragmentMatchesWriter(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name"}
	values := []spanner.GenericColumnValue{gcvctor.Int64Value(42), gcvctor.StringValue("Alice")}
	formatter := spanvalue.LiteralFormatConfig()

	var writerOut bytes.Buffer
	w := mustNewSQLInsertWriter(t, &writerOut, "users")
	if err := w.WriteValues(columnNames, values); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	formatted, err := spanvalue.FormatRowColumns(formatter, columnNames, values)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", columnNames)
	if err != nil {
		t.Fatal(err)
	}
	got := FormatSQLInsertStatement(prefix, formatted)
	if diff := cmp.Diff(writerOut.String(), got); diff != "" {
		t.Fatalf("fragment assembly mismatch (-writer +fragment):\n%s", diff)
	}
}

func TestFormatSQLInsertFragmentBatchMatchesWriter(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name"}
	rows := [][]spanner.GenericColumnValue{
		{gcvctor.Int64Value(1), gcvctor.StringValue("a")},
		{gcvctor.Int64Value(2), gcvctor.StringValue("b")},
		{gcvctor.Int64Value(3), gcvctor.StringValue("c")},
	}
	formatter := spanvalue.LiteralFormatConfig()

	var writerOut bytes.Buffer
	w := mustNewSQLInsertWriter(t, &writerOut, "users", WithSQLBatchSize(2))
	tuples := make([]string, 0, len(rows))
	for _, values := range rows {
		if err := w.WriteValues(columnNames, values); err != nil {
			t.Fatal(err)
		}
		formatted, err := spanvalue.FormatRowColumns(formatter, columnNames, values)
		if err != nil {
			t.Fatal(err)
		}
		tuples = append(tuples, FormatSQLInsertValuesTuple(formatted))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	prefix, err := FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", columnNames)
	if err != nil {
		t.Fatal(err)
	}
	first, err := FormatSQLInsertBatch(prefix, tuples[:2])
	if err != nil {
		t.Fatal(err)
	}
	rest, err := FormatSQLInsertBatch(prefix, tuples[2:])
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(writerOut.String(), first+rest); diff != "" {
		t.Fatalf("batch assembly mismatch (-writer +fragment):\n%s", diff)
	}
}

func TestFormatSQLInsertPrefixKindsAndDialect(t *testing.T) {
	t.Parallel()

	prefix, err := FormatSQLInsertPrefix(SQLInsertOrIgnore, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff("INSERT OR IGNORE INTO `users` (`id`) VALUES", prefix); diff != "" {
		t.Fatal(diff)
	}

	_, err = FormatSQLInsertPrefix(SQLInsertOrUpdate, databasepb.DatabaseDialect_POSTGRESQL, "users", []string{"id"})
	if !errors.Is(err, ErrInvalidSQLInsertKindForDialect) {
		t.Fatalf("error = %v, want ErrInvalidSQLInsertKindForDialect", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsertKind(9), databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", []string{"id"})
	if !errors.Is(err, ErrInvalidSQLInsertKind) {
		t.Fatalf("error = %v, want ErrInvalidSQLInsertKind", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "  ", nil)
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("error = %v, want ErrEmptyTableName", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "users", nil)
	if !errors.Is(err, ErrEmptyColumnName) {
		t.Fatalf("error = %v, want ErrEmptyColumnName", err)
	}

	pg, err := FormatSQLInsertPrefix(SQLInsert, databasepb.DatabaseDialect_POSTGRESQL, "users", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(`INSERT INTO "users" ("id") VALUES`, pg); diff != "" {
		t.Fatal(diff)
	}
}
