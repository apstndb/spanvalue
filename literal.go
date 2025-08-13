package spanvalue

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"

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
		formatUUID(),
		FormatProtoAsCast,
		FormatEnumAsCast,
	},
}

// formatUUID is workaround because google-cloud-go/spanner doesn't yet support UUID type.
func formatUUID() FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if value.Type.GetCode() != sppb.TypeCode_UUID {
			return "", ErrFallthrough
		}

		if _, ok := value.Value.Kind.(*structpb.Value_NullValue); ok {
			return "NULL", nil
		}

		return fmt.Sprintf("CAST(%q AS UUID)", value.Value.GetStringValue()), nil
	}
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

func formatNullableValueLiteral(value NullableValue) (string, error) {
	switch v := value.(type) {
	case spanner.NullString:
		return strconv.Quote(v.StringVal), nil
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
		return fmt.Sprintf("NUMERIC %q", spanner.NumericString(&v.Numeric)), nil
	case spanner.NullTime:
		return fmt.Sprintf("TIMESTAMP %q", v.Time.Format(time.RFC3339Nano)), nil
	case spanner.NullDate:
		return fmt.Sprintf("DATE %q", v.Date.String()), nil
	case spanner.NullJSON:
		return fmt.Sprintf("JSON %q", v.String()), nil
	case spanner.NullInterval:
		// Use CAST for INTERVAL. Literal notation is unintuitive for information preservation.
		return fmt.Sprintf("CAST(%q AS INTERVAL)", v.String()), nil
	default:
		// Reject unknown type to guarantee round-trip
		return "", errors.New("unknown type")
	}
}
