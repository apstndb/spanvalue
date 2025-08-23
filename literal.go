package spanvalue

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue/internal"
)

func FormatRowLiteral(value *spanner.Row) ([]string, error) {
	return LiteralFormatConfig.FormatRow(value)
}

func FormatColumnLiteral(value spanner.GenericColumnValue) (string, error) {
	return LiteralFormatConfig.FormatToplevelColumn(value)
}

var LiteralFormatConfig = &FormatConfig{
	NullString:     nullStringUpperCase,
	FormatArray:    FormatOptionallyTypedArray,
	FormatStruct:   FormatTypedStruct,
	FormatNullable: formatNullableValueLiteral,
	FormatComplexPlugins: []FormatComplexFunc{
		FormatProtoAsCast,
		FormatEnumAsCast,
	},
}

var (
	_ FormatStructParenFunc = formatTypedStructParen
	_ FormatStructParenFunc = FormatTupleStruct
	_ FormatStructFieldFunc = formatSimpleStructField
	_ FormatStructFieldFunc = FormatTypelessStructField
)

var (
	_ FormatNullableFunc = formatNullableValueLiteral
)

func stringBasedLiteral(typ string, s string) string {
	return fmt.Sprintf("%s %s", typ, internal.ToStringLiteral(s))
}

func stringLiteralCast(typ string, s string) string {
	return fmt.Sprintf("CAST(%s AS %s)", internal.ToStringLiteral(s), typ)
}

func formatNullableValueLiteral(value NullableValue) (string, error) {
	switch v := value.(type) {
	case spanner.NullString:
		return internal.ToStringLiteral(v.StringVal), nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case NullBytes:
		return internal.ToReadableBytesLiteral(v), nil
	case spanner.NullFloat32:
		return internal.Float32ToLiteral(v.Float32), nil
	case spanner.NullFloat64:
		return internal.Float64ToLiteral(v.Float64), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return stringBasedLiteral("NUMERIC", spanner.NumericString(&v.Numeric)), nil
	case spanner.NullTime:
		return stringBasedLiteral("TIMESTAMP", v.Time.Format(time.RFC3339Nano)), nil
	case spanner.NullDate:
		return stringBasedLiteral("DATE", v.Date.String()), nil
	case spanner.NullJSON:
		return stringBasedLiteral("JSON", v.String()), nil
	case spanner.NullInterval:
		// Use CAST for INTERVAL. Literal notation is unintuitive for information preservation.
		return stringLiteralCast("INTERVAL", v.String()), nil
	case spanner.NullUUID:
		return stringLiteralCast("UUID", v.String()), nil
	default:
		// Reject unknown type to guarantee round-trip
		return "", errors.New("unknown type")
	}
}
