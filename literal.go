package spanvalue

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue/internal"
)

// literalFormatConfig is a shared singleton used by convenience functions
// (FormatRowLiteral, FormatColumnLiteral) to avoid per-call allocation.
// Do not mutate: it is shared across all callers.
var literalFormatConfig = LiteralFormatConfig()

func FormatRowLiteral(value *spanner.Row) ([]string, error) {
	return literalFormatConfig.FormatRow(value)
}

func FormatColumnLiteral(value spanner.GenericColumnValue) (string, error) {
	return literalFormatConfig.FormatToplevelColumn(value)
}

// LiteralFormatConfig returns a new FormatConfig that produces parseable SQL
// literal expressions with type annotations. ARRAY values use
// [FormatOptionallyTypedArray]: top-level arrays with scalar elements omit the
// ARRAY<...> prefix (empty or not); arrays of STRUCT or nested ARRAY include it when
// toplevel is true (empty or not).
func LiteralFormatConfig() *FormatConfig {
	return &FormatConfig{
		NullString:     nullStringUpperCase,
		FormatArray:    FormatOptionallyTypedArray,
		FormatStruct:   TypedStructFormat(),
		FormatNullable: formatNullableValueLiteral,
		FormatComplexPlugins: []FormatComplexFunc{
			FormatProtoAsCast,
			FormatEnumAsCast,
			FormatLiteralValue,
		},
	}
}

var _ func() *FormatConfig = LiteralFormatConfig

var (
	_ FormatStructParenFunc = formatTypedStructParen
	_ FormatStructParenFunc = FormatTupleStruct
	_ FormatStructFieldFunc = formatSimpleStructField
	_ FormatStructFieldFunc = FormatTypelessStructField
)

var (
	_ FormatNullableFunc = formatNullableValueLiteral
)

func stringBasedLiteral(typ, s string, policy internal.QuotePolicy) string {
	return fmt.Sprintf("%s %s", typ, internal.ToStringLiteralPolicy(s, policy))
}

func stringLiteralCast(typ, s string, policy internal.QuotePolicy) string {
	return fmt.Sprintf("CAST(%s AS %s)", internal.ToStringLiteralPolicy(s, policy), typ)
}

// formatNullableValueLiteral is the identity sentinel for scalarFastPathActive and
// formatSimpleColumn dispatch. It must stay equivalent to
// formatNullableValueLiteralWithQuote(LiteralQuoteConfig{}, nv).
func formatNullableValueLiteral(value NullableValue) (string, error) {
	return formatNullableValueLiteralWithQuote(LiteralQuoteConfig{}, value)
}

func formatNullableValueLiteralWithQuote(q LiteralQuoteConfig, value NullableValue) (string, error) {
	policy := toInternalQuotePolicy(q)
	switch v := value.(type) {
	case spanner.NullString:
		return internal.ToStringLiteralPolicy(v.StringVal, policy), nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case NullBytes:
		return internal.ToReadableBytesLiteralPolicy(v, policy), nil
	case spanner.NullFloat32:
		return internal.Float32ToLiteralPolicy(v.Float32, policy), nil
	case spanner.NullFloat64:
		return internal.Float64ToLiteralPolicy(v.Float64, policy), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return stringBasedLiteral("NUMERIC", spanner.NumericString(&v.Numeric), policy), nil
	case spanner.PGNumeric:
		return stringBasedLiteral("NUMERIC", v.Numeric, policy), nil
	case spanner.NullTime:
		return stringBasedLiteral("TIMESTAMP", v.Time.Format(time.RFC3339Nano), policy), nil
	case spanner.NullDate:
		return stringBasedLiteral("DATE", v.Date.String(), policy), nil
	case spanner.NullJSON:
		return stringBasedLiteral("JSON", v.String(), policy), nil
	case spanner.PGJsonB:
		b, err := json.Marshal(v.Value)
		if err != nil {
			return "", err
		}
		return stringBasedLiteral("JSON", string(b), policy), nil
	case spanner.NullInterval:
		// Use CAST for INTERVAL. Literal notation is unintuitive for information preservation.
		return stringLiteralCast("INTERVAL", v.String(), policy), nil
	case spanner.NullUUID:
		return stringLiteralCast("UUID", v.String(), policy), nil
	default:
		// Reject unknown type to guarantee round-trip
		return "", fmt.Errorf("%w: %T", ErrUnknownType, v)
	}
}
