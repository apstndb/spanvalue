package spanvalue

import (
	"testing"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect databasepb.DatabaseDialect
		input   string
		want    string
	}{
		{"GoogleSQL simple", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "table", "`table`"},
		{"GoogleSQL plain name unchanged", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "Singers", "`Singers`"},
		{"GoogleSQL escapes backtick", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "a`b", "`a\\`b`"},
		{"GoogleSQL escapes trailing backslash", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, `a\`, "`a\\\\`"},
		{"GoogleSQL escapes backslash then backtick", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "a\\`b", "`a\\\\\\`b`"},
		{"PostgreSQL simple", databasepb.DatabaseDialect_POSTGRESQL, "table", `"table"`},
		{"PostgreSQL escapes quote", databasepb.DatabaseDialect_POSTGRESQL, `a"b`, `"a""b"`},
		{"Unspecified defaults to GoogleSQL", databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, "table", "`table`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := QuoteIdentifier(tt.dialect, tt.input); got != tt.want {
				t.Fatalf("QuoteIdentifier(%v, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect databasepb.DatabaseDialect
		input   string
		want    string
	}{
		{"GoogleSQL qualified", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "schema.table", "`schema`.`table`"},
		{"PostgreSQL qualified", databasepb.DatabaseDialect_POSTGRESQL, "schema.table", `"schema"."table"`},
		{"GoogleSQL preserves empty segment shape", databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, "schema..table", "`schema`.``.`table`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := QuoteQualifiedIdentifier(tt.dialect, tt.input); got != tt.want {
				t.Fatalf("QuoteQualifiedIdentifier(%v, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}
