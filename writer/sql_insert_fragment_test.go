package writer

import (
	"errors"
	"strings"
	"testing"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/go-cmp/cmp"
)

func TestFormatSQLInsertFragmentsGolden(t *testing.T) {
	t.Parallel()

	gs := databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL
	pg := databasepb.DatabaseDialect_POSTGRESQL
	unspec := databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED

	prefix := func(kind SQLInsertKind, dialect databasepb.DatabaseDialect, table string, cols []string) string {
		t.Helper()
		got, err := FormatSQLInsertPrefix(kind, dialect, table, cols)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("single row", func(t *testing.T) {
		t.Parallel()
		p := prefix(SQLInsert, gs, "users", []string{"id", "name"})
		got := FormatSQLInsertStatement(p, []string{"1", `"a"`})
		want := "INSERT INTO `users` (`id`, `name`) VALUES (1, \"a\");\n"
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("caller assembled batch and remainder", func(t *testing.T) {
		t.Parallel()
		p := prefix(SQLInsert, gs, "users", []string{"id", "name"})
		row := func(id, name string) string {
			return FormatSQLInsertValuesTuple([]string{id, name})
		}
		batch := p + "\n  " + row("1", `"a"`) + ",\n  " + row("2", `"b"`) + ";\n"
		rest := p + "\n  " + row("3", `"c"`) + ";\n"
		want := "" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (1, \"a\"),\n" +
			"  (2, \"b\");\n" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (3, \"c\");\n"
		if diff := cmp.Diff(want, batch+rest); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("postgresql literals stay caller text", func(t *testing.T) {
		t.Parallel()
		p := prefix(SQLInsert, pg, "users", []string{"id", "name"})
		got := FormatSQLInsertStatement(p, []string{"1", "'a'"})
		want := "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a');\n"
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("qualified and escaped identifiers", func(t *testing.T) {
		t.Parallel()
		got := prefix(SQLInsert, gs, "db.users", []string{"a`b"})
		want := "INSERT INTO `db`.`users` (`a\\`b`) VALUES"
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("google SQL kinds", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			kind SQLInsertKind
			want string
		}{
			{SQLInsert, "INSERT INTO `users` (`id`) VALUES"},
			{SQLInsertOrIgnore, "INSERT OR IGNORE INTO `users` (`id`) VALUES"},
			{SQLInsertOrUpdate, "INSERT OR UPDATE INTO `users` (`id`) VALUES"},
		}
		for _, tt := range cases {
			got := prefix(tt.kind, gs, "users", []string{"id"})
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		}
	})

	t.Run("unspecified dialect matches GoogleSQL quoting", func(t *testing.T) {
		t.Parallel()
		got := prefix(SQLInsert, unspec, "users", []string{"id"})
		want := "INSERT INTO `users` (`id`) VALUES"
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestFormatSQLInsertPrefixErrors(t *testing.T) {
	t.Parallel()

	gs := databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL
	pg := databasepb.DatabaseDialect_POSTGRESQL

	_, err := FormatSQLInsertPrefix(SQLInsertOrIgnore, pg, "users", []string{"id"})
	if !errors.Is(err, ErrInvalidSQLInsertKindForDialect) {
		t.Fatalf("OR IGNORE error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsertOrUpdate, pg, "users", []string{"id"})
	if !errors.Is(err, ErrInvalidSQLInsertKindForDialect) {
		t.Fatalf("OR UPDATE error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsertKind(9), gs, "users", []string{"id"})
	if !errors.Is(err, ErrInvalidSQLInsertKind) {
		t.Fatalf("invalid kind error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, gs, "  ", []string{"id"})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("blank table error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, gs, "db..users", []string{"id"})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("empty segment error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, gs, "users", nil)
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("nil columns error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, gs, "users", []string{})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("empty columns error = %v", err)
	}
	_, err = FormatSQLInsertPrefix(SQLInsert, gs, "users", []string{"id", ""})
	if !errors.Is(err, ErrEmptyColumnName) || errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("empty column element error = %v", err)
	}
}

func TestFormatSQLInsertTupleDoesNotValidateWidth(t *testing.T) {
	t.Parallel()

	got := FormatSQLInsertValuesTuple(nil)
	if got != "()" {
		t.Fatalf("tuple = %q, want ()", got)
	}
	stmt := FormatSQLInsertStatement("INSERT INTO `t` (`a`, `b`) VALUES", []string{"1"})
	if !strings.HasSuffix(stmt, " (1);\n") {
		t.Fatalf("statement = %q", stmt)
	}
}
