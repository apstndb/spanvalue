package writer

import (
	"fmt"
	"strings"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// FormatSQLInsertPrefix returns the INSERT statement prefix through the VALUES
// keyword, before the parenthesized tuple from [FormatSQLInsertValuesTuple], for example:
//
//	INSERT INTO `users` (`id`, `name`) VALUES
//
// FormatSQLInsertPrefix is a WIP scaffold for custom batching layers (#79). It mirrors
// [SQLInsertWriter] identifier quoting and INSERT kind prefixes but does not format
// cell values; callers supply literals via [FormatSQLInsertValuesTuple] or their own
// formatter. API shape and batching helpers may change before stabilization.
func FormatSQLInsertPrefix(kind SQLInsertKind, dialect databasepb.DatabaseDialect, table string, columnNames []string) (string, error) {
	if table == "" {
		return "", ErrEmptyTableName
	}
	quotedTable, err := quoteQualifiedIdentifier(table, dialect)
	if err != nil {
		return "", err
	}
	quotedColumns, err := quoteIdentifiers(columnNames, dialect)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s INTO %s (%s) VALUES", kind.String(), quotedTable, strings.Join(quotedColumns, ", ")), nil
}

// FormatSQLInsertValuesTuple formats a parenthesized, comma-separated VALUES tuple
// from pre-formatted column literals, for example (42, "Alice").
func FormatSQLInsertValuesTuple(formattedValues []string) string {
	var b strings.Builder
	b.WriteByte('(')
	for i, val := range formattedValues {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(val)
	}
	b.WriteByte(')')
	return b.String()
}

// FormatSQLInsertStatement assembles a complete single-row INSERT statement from a
// prefix, a parenthesized value tuple, and the statement terminator.
func FormatSQLInsertStatement(prefix string, formattedValues []string) string {
	return prefix + " " + FormatSQLInsertValuesTuple(formattedValues) + ";\n"
}
