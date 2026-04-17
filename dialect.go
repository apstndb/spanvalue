package spanvalue

import "strings"

// SQLDialect selects SQL-surface details such as identifier quoting rules.
type SQLDialect string

const (
	SQLDialectGoogleSQL  SQLDialect = "googlesql"
	SQLDialectPostgreSQL SQLDialect = "postgresql"
)

// QuoteIdentifier quotes a single identifier for dialect.
func QuoteIdentifier(dialect SQLDialect, name string) string {
	switch dialect {
	case SQLDialectPostgreSQL:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	default:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
}

// QuoteQualifiedIdentifier quotes each segment of a dotted identifier path for dialect.
func QuoteQualifiedIdentifier(dialect SQLDialect, name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = QuoteIdentifier(dialect, part)
	}
	return strings.Join(parts, ".")
}
