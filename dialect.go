package spanvalue

import (
	"strings"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// googleSQLIdentifierEscaper escapes the characters that must be escaped inside a
// GoogleSQL backquoted identifier. Quoted identifiers use the same escape sequences
// as string literals, so backslash becomes \\ and backtick becomes \`.
// strings.Replacer replaces in a single pass, so the backslash escape never
// re-processes the backslash introduced by the backtick escape (and vice versa).
var googleSQLIdentifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// QuoteIdentifier quotes a single identifier for dialect.
// For GoogleSQL it escapes the identifier with string-literal escape sequences
// (backslash to \\ and backtick to \`) per the GoogleSQL lexical structure;
// for PostgreSQL it doubles embedded double quotes.
// It does not validate the identifier; for example, an empty string becomes an empty quoted
// identifier (two backticks for GoogleSQL, "" for PostgreSQL), which is invalid SQL in both dialects.
// DATABASE_DIALECT_UNSPECIFIED follows the Spanner default and uses GoogleSQL quoting.
func QuoteIdentifier(dialect databasepb.DatabaseDialect, name string) string {
	switch dialect {
	case databasepb.DatabaseDialect_POSTGRESQL:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	default:
		return "`" + googleSQLIdentifierEscaper.Replace(name) + "`"
	}
}

// QuoteQualifiedIdentifier quotes each segment of a dotted identifier path for dialect.
// It does not validate the path; callers that reject empty segments must do so before calling it.
func QuoteQualifiedIdentifier(dialect databasepb.DatabaseDialect, name string) string {
	if !strings.Contains(name, ".") {
		return QuoteIdentifier(dialect, name)
	}
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = QuoteIdentifier(dialect, part)
	}
	return strings.Join(parts, ".")
}
