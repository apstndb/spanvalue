package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestSQLInsertWriterWithTablePlaceholder_scaffold(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "",
		WithSQLTablePlaceholder("TABLE_NAME"),
	)
	if err := w.WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1)}); err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}

	want := "INSERT INTO TABLE_NAME (`id`) VALUES (1);\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLInsertWriterWithTablePlaceholder_postgresColumnsQuoted(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "",
		WithSQLTablePlaceholder("/* unresolved table */"),
		WithSQLDialect(databasepb.DatabaseDialect_POSTGRESQL),
	)
	if err := w.WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(42)}); err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}

	want := "INSERT INTO /* unresolved table */ (\"id\") VALUES (42);\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLInsertWriterWithTablePlaceholder_emptyTokenRejected(t *testing.T) {
	t.Parallel()

	_, err := NewSQLInsertWriter(&bytes.Buffer{}, "users", WithSQLTablePlaceholder(""))
	if !errors.Is(err, ErrEmptyTablePlaceholder) {
		t.Fatalf("NewSQLInsertWriter() error = %v, want ErrEmptyTablePlaceholder", err)
	}
}

func TestSQLInsertWriterEmptyTableWithoutPlaceholder(t *testing.T) {
	t.Parallel()
	t.Skip("WIP: #147 will reject empty table at construction; placeholder path should remain valid")

	var out bytes.Buffer
	_, err := NewSQLInsertWriter(&out, "")
	if err != nil {
		t.Fatalf("NewSQLInsertWriter() error = %v, want nil until #147 lands", err)
	}
	err = mustNewSQLInsertWriter(t, &out, "").WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1)})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("WriteValues() error = %v, want ErrEmptyTableName", err)
	}
}

func TestSQLInsertWriterTablePlaceholder_batchedGoldenSketch(t *testing.T) {
	t.Parallel()
	t.Skip("WIP #146: batched INSERT with placeholders not golden-tested yet")

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "",
		WithSQLTablePlaceholder("TABLE_NAME"),
		WithSQLBatchSize(2),
	)
	for _, id := range []int64{1, 2} {
		if err := w.WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(id)}); err != nil {
			t.Fatalf("WriteValues() error = %v", err)
		}
	}
	want := "INSERT INTO TABLE_NAME (`id`) VALUES\n  (1),\n  (2);\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}
