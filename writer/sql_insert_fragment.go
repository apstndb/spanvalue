package writer

import (
	"fmt"
	"strings"

	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// FormatSQLInsertPrefix returns the INSERT statement prefix through the VALUES
// keyword, before tuples from [FormatSQLInsertValuesTuple]. For example:
//
//	INSERT INTO `users` (`id`, `name`) VALUES
//
// kind selects INSERT, INSERT OR IGNORE, or INSERT OR UPDATE. The same
// dialect rules as [SQLInsertWriter] apply: an unknown kind is
// [ErrInvalidSQLInsertKind], and INSERT OR IGNORE / INSERT OR UPDATE with
// PostgreSQL is [ErrInvalidSQLInsertKindForDialect]. There is no separate
// helper per kind.
//
// The prefix does not format cell values. Callers pass already formatted
// literals to [FormatSQLInsertValuesTuple], usually from
// [github.com/apstndb/spanvalue.FormatRowColumns]. [FormatSQLInsertStatement] and
// [FormatSQLInsertBatch] only concatenate those pieces. They do not choose
// a batch size; the caller does.
func FormatSQLInsertPrefix(kind SQLInsertKind, dialect databasepb.DatabaseDialect, table string, columnNames []string) (string, error) {
	if err := (&SQLInsertWriter{insertKind: kind, sqlDialect: dialect}).validateSQLInsertConfig(); err != nil {
		return "", err
	}
	if strings.TrimSpace(table) == "" {
		return "", ErrEmptyTableName
	}
	if len(columnNames) == 0 {
		return "", ErrEmptyColumnName
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
// pre-formatted column literals, for example (42, "Alice").
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
func FormatSQLInsertStatement(prefix string, formattedValues []string) string {
	return prefix + " " + FormatSQLInsertValuesTuple(formattedValues) + ";\n"
}

// FormatSQLInsertBatch assembles one batched INSERT statement from tuples
// produced by [FormatSQLInsertValuesTuple]. The layout matches [SQLInsertWriter]
// when [WithSQLBatchSize] is greater than one, including a flushed remainder
// of a single tuple:
//
//	INSERT INTO `users` (`id`) VALUES
//	  (1),
//	  (2);
//
// Single-row statements that put the tuple on the VALUES line use
// [FormatSQLInsertStatement] instead. An empty tuple list is an error. The
// caller decides how many tuples belong in the statement.
func FormatSQLInsertBatch(prefix string, tuples []string) (string, error) {
	if len(tuples) == 0 {
		return "", fmt.Errorf("sql insert batch: no value tuples")
	}
	return prefix + "\n  " + strings.Join(tuples, ",\n  ") + ";\n", nil
}
