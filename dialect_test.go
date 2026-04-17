package spanvalue

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect SQLDialect
		input   string
		want    string
	}{
		{"GoogleSQL simple", SQLDialectGoogleSQL, "table", "`table`"},
		{"GoogleSQL escapes backtick", SQLDialectGoogleSQL, "a`b", "`a``b`"},
		{"PostgreSQL simple", SQLDialectPostgreSQL, "table", `"table"`},
		{"PostgreSQL escapes quote", SQLDialectPostgreSQL, `a"b`, `"a""b"`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := QuoteIdentifier(tt.dialect, tt.input); got != tt.want {
				t.Fatalf("QuoteIdentifier(%q, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect SQLDialect
		input   string
		want    string
	}{
		{"GoogleSQL qualified", SQLDialectGoogleSQL, "schema.table", "`schema`.`table`"},
		{"PostgreSQL qualified", SQLDialectPostgreSQL, "schema.table", `"schema"."table"`},
		{"GoogleSQL preserves empty segment shape", SQLDialectGoogleSQL, "schema..table", "`schema`.``.`table`"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := QuoteQualifiedIdentifier(tt.dialect, tt.input); got != tt.want {
				t.Fatalf("QuoteQualifiedIdentifier(%q, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}
