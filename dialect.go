package spanvalue

import (
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// SQLDialect selects SQL surface details such as identifier quoting.
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

// QuoteQualifiedIdentifier quotes each segment of a dotted path for dialect.
func QuoteQualifiedIdentifier(dialect SQLDialect, name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = QuoteIdentifier(dialect, part)
	}
	return strings.Join(parts, ".")
}

func formatPostgreSQLType(typ *sppb.Type) string {
	if typ == nil {
		return ""
	}

	switch typ.GetCode() {
	case sppb.TypeCode_BOOL:
		return "bool"
	case sppb.TypeCode_BYTES:
		return "bytea"
	case sppb.TypeCode_DATE:
		return "date"
	case sppb.TypeCode_FLOAT32:
		return "float4"
	case sppb.TypeCode_FLOAT64:
		return "float8"
	case sppb.TypeCode_INT64:
		return "bigint"
	case sppb.TypeCode_INTERVAL:
		return "interval"
	case sppb.TypeCode_JSON:
		if typ.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return "jsonb"
		}
		return "json"
	case sppb.TypeCode_NUMERIC:
		return "numeric"
	case sppb.TypeCode_STRING:
		return "text"
	case sppb.TypeCode_TIMESTAMP:
		return "timestamptz"
	case sppb.TypeCode_UUID:
		return "uuid"
	case sppb.TypeCode_ENUM:
		if typeName := typ.GetProtoTypeFqn(); typeName != "" {
			return QuoteQualifiedIdentifier(SQLDialectPostgreSQL, typeName)
		}
		return "enum"
	case sppb.TypeCode_PROTO:
		if typeName := typ.GetProtoTypeFqn(); typeName != "" {
			return QuoteQualifiedIdentifier(SQLDialectPostgreSQL, typeName)
		}
		return "proto"
	case sppb.TypeCode_ARRAY:
		return formatPostgreSQLType(typ.GetArrayElementType()) + "[]"
	case sppb.TypeCode_STRUCT:
		// PostgreSQL dialect does not expose Spanner STRUCT/PROTO/ENUM as first-class
		// SQL types. PostgreSQLLiteralFormatConfig rejects such values before generating
		// literals; this fallback keeps the helper usable for diagnostics without
		// pretending STRUCT<...> is valid PostgreSQL syntax.
		return "record"
	default:
		return ""
	}
}
