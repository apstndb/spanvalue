package spanvalue

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// postgreSQLLiteralFormatConfig is a shared singleton used by convenience functions
// (FormatRowPostgreSQLLiteral, FormatColumnPostgreSQLLiteral) to avoid per-call allocation.
// Do not mutate: it is shared across all callers.
var postgreSQLLiteralFormatConfig = PostgreSQLLiteralFormatConfig()

// PostgreSQLLiteralFormatConfig returns a new FormatConfig that produces parseable
// PostgreSQL-dialect literal expressions for scalar values plus ARRAY/ROW constructors.
func PostgreSQLLiteralFormatConfig() *FormatConfig {
	return &FormatConfig{
		NullString:  nullStringUpperCase,
		FormatArray: FormatPostgreSQLArray,
		FormatStruct: FormatStruct{
			FormatStructField: FormatSimpleStructField,
			FormatStructParen: FormatPostgreSQLStruct,
		},
		FormatNullable: formatNullableValuePostgreSQLLiteral,
	}
}

// FormatRowPostgreSQLLiteral formats a row using PostgreSQLLiteralFormatConfig.
func FormatRowPostgreSQLLiteral(value *spanner.Row) ([]string, error) {
	return postgreSQLLiteralFormatConfig.FormatRow(value)
}

// FormatColumnPostgreSQLLiteral formats a top-level column using PostgreSQLLiteralFormatConfig.
func FormatColumnPostgreSQLLiteral(value spanner.GenericColumnValue) (string, error) {
	return postgreSQLLiteralFormatConfig.FormatToplevelColumn(value)
}

// FormatPostgreSQLArray formats ARRAY values using PostgreSQL ARRAY constructors.
func FormatPostgreSQLArray(typ *sppb.Type, _ bool, elemStrings []string) (string, error) {
	if len(elemStrings) == 0 {
		return fmt.Sprintf("CAST(ARRAY[] AS %s)", formatPostgreSQLType(typ)), nil
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(elemStrings, ", ")), nil
}

// FormatPostgreSQLStruct formats STRUCT-like values using PostgreSQL ROW constructors.
func FormatPostgreSQLStruct(_ *sppb.Type, _ bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("ROW(%s)", strings.Join(fieldStrings, ", ")), nil
}

func postgreSQLStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func postgreSQLCast(expr string, typ string) string {
	return fmt.Sprintf("CAST(%s AS %s)", expr, typ)
}

func postgreSQLFloatLiteral(v float64, bits int) string {
	typ := "float8"
	if bits == 32 {
		typ = "float4"
	}
	switch {
	case math.IsNaN(v):
		return postgreSQLCast(postgreSQLStringLiteral("NaN"), typ)
	case math.IsInf(v, 1):
		return postgreSQLCast(postgreSQLStringLiteral("Infinity"), typ)
	case math.IsInf(v, -1):
		return postgreSQLCast(postgreSQLStringLiteral("-Infinity"), typ)
	default:
		return postgreSQLCast(strconv.FormatFloat(v, 'g', -1, bits), typ)
	}
}

func formatNullableValuePostgreSQLLiteral(value NullableValue) (string, error) {
	switch v := value.(type) {
	case spanner.NullString:
		return postgreSQLStringLiteral(v.StringVal), nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case NullBytes:
		return postgreSQLCast(postgreSQLStringLiteral(`\x`+hex.EncodeToString(v)), "bytea"), nil
	case spanner.NullFloat32:
		return postgreSQLFloatLiteral(float64(v.Float32), 32), nil
	case spanner.NullFloat64:
		return postgreSQLFloatLiteral(v.Float64, 64), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return postgreSQLCast(postgreSQLStringLiteral(spanner.NumericString(&v.Numeric)), "numeric"), nil
	case spanner.PGNumeric:
		return postgreSQLCast(postgreSQLStringLiteral(v.Numeric), "numeric"), nil
	case spanner.NullTime:
		return postgreSQLCast(postgreSQLStringLiteral(v.Time.UTC().Format(time.RFC3339Nano)), "timestamptz"), nil
	case spanner.NullDate:
		return postgreSQLCast(postgreSQLStringLiteral(v.Date.String()), "date"), nil
	case spanner.NullJSON:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "json"), nil
	case spanner.PGJsonB:
		b, err := json.Marshal(v.Value)
		if err != nil {
			return "", err
		}
		return postgreSQLCast(postgreSQLStringLiteral(string(b)), "jsonb"), nil
	case spanner.NullInterval:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "interval"), nil
	case spanner.NullUUID:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "uuid"), nil
	default:
		return "", fmt.Errorf("%w: %T", ErrUnknownType, v)
	}
}

var (
	_ FormatArrayFunc       = FormatPostgreSQLArray
	_ FormatStructParenFunc = FormatPostgreSQLStruct
	_ FormatNullableFunc    = formatNullableValuePostgreSQLLiteral
)
