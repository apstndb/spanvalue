package writer

import (
	"fmt"
	"strings"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// FormatSQLInsertPrefix returns the INSERT statement prefix through the VALUES
// keyword. For example:
//
//	INSERT INTO `users` (`id`, `name`) VALUES
//
// kind selects INSERT, INSERT OR IGNORE, or INSERT OR UPDATE. There is no
// sibling helper per kind. The same dialect rules as [SQLInsertWriter] apply:
// an unknown kind is [ErrInvalidSQLInsertKind], and INSERT OR IGNORE or
// INSERT OR UPDATE with PostgreSQL is [ErrInvalidSQLInsertKindForDialect].
// DATABASE_DIALECT_UNSPECIFIED and any other non-PostgreSQL dialect use
// GoogleSQL identifier quoting, matching
// [github.com/apstndb/spanvalue.QuoteIdentifier].
//
// A blank table name, including whitespace only, is [ErrEmptyTableName].
// A qualified name with an empty segment, such as "db..users", is also
// [ErrEmptyTableName]. A nil or empty column list is [ErrMissingColumnNames].
// An empty column name inside a non-empty list is [ErrEmptyColumnName].
// Those are different failures: the list is absent, or one of its names is empty.
//
// The prefix does not format cell values. Callers pass already formatted
// literals to [FormatSQLInsertValuesTuple], usually from
// [github.com/apstndb/spanvalue.FormatRowColumns]. [FormatSQLInsertStatement]
// only concatenates those pieces. It does not parse SQL or check that the
// number of literals matches the column list. The caller owns batch size and
// joins tuples.
func FormatSQLInsertPrefix(kind SQLInsertKind, dialect databasepb.DatabaseDialect, table string, columnNames []string) (string, error) {
	if err := (&SQLInsertWriter{insertKind: kind, sqlDialect: dialect}).validateSQLInsertConfig(); err != nil {
		return "", err
	}
	if strings.TrimSpace(table) == "" {
		return "", ErrEmptyTableName
	}
	if len(columnNames) == 0 {
		return "", ErrMissingColumnNames
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

// FormatSQLInsertValuesTuple formats one parenthesized VALUES tuple from
// pre-formatted column literals, for example (42, "Alice"). It does not parse
// the literals or check how many there are. Callers must pass a nonempty list
// whose width already matches the INSERT column list.
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

// FormatSQLInsertStatement assembles one single-row INSERT statement.
// formattedValues are cell literals, not a tuple that already has parentheses.
// The helper does not parse SQL or validate the row width. Callers who batch
// several tuples join those tuples themselves; see ExampleFormatSQLInsertPrefix.
func FormatSQLInsertStatement(prefix string, formattedValues []string) string {
	return prefix + " " + FormatSQLInsertValuesTuple(formattedValues) + ";\n"
}
