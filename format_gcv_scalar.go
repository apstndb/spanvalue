package spanvalue

import (
	"fmt"
	"math"
	"strconv"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/internal"
)

var (
	_ FormatComplexFunc = FormatSimpleValue
	_ FormatComplexFunc = FormatSpannerCLIValue
	_ FormatComplexFunc = FormatJSONSimpleValue
)

// scalarTypeCode returns the column type code, or ok false to fall through when Type is nil.
func scalarTypeCode(value spanner.GenericColumnValue) (sppb.TypeCode, bool) {
	if value.Type == nil {
		return 0, false
	}
	return value.Type.GetCode(), true
}

// FormatSimpleValue is a [FormatComplexFunc] that formats scalars for
// [SimpleFormatConfig] without constructing a [NullableValue]. It returns
// [ErrFallthrough] for ARRAY, STRUCT, and type codes outside the supported
// scalar set ([isScalarFastPathTypeCode]). NUMERIC uses the string wire
// payload as-is; canonical wire is the GCV constructor's responsibility (see
// [github.com/apstndb/spanvalue/gcvctor.StringBasedValueFromCode]).
func FormatSimpleValue(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	code, ok := scalarTypeCode(value)
	if !ok {
		return "", ErrFallthrough
	}
	switch code {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT:
		return "", ErrFallthrough
	}
	if !isScalarFastPathTypeCode(code) {
		return "", ErrFallthrough
	}
	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	return formatGCVScalarSimple(value)
}

// LiteralValuePlugin returns the literal preset's scalar [FormatComplexFunc]
// with quote options captured at construction (invalid enum values are
// normalized). It returns [ErrFallthrough] for ARRAY, STRUCT, PROTO, and ENUM
// — the literal preset handles PROTO/ENUM with [FormatProtoAsCast] and
// [FormatEnumAsCast] earlier in the chain — and for type codes outside the
// supported scalar set.
//
// LiteralValuePlugin replaces the pre-v0.8 exported plugin value
// FormatLiteralValue, which read quote options from the removed
// FormatConfig.Literal field; LiteralValuePlugin(LiteralFormatOptions{}) is
// the default-quote equivalent.
func LiteralValuePlugin(opts LiteralFormatOptions) FormatComplexFunc {
	q := normalizeLiteralQuote(opts.Quote)
	return func(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		code, ok := scalarTypeCode(value)
		if !ok {
			return "", ErrFallthrough
		}
		switch code {
		case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT, sppb.TypeCode_PROTO, sppb.TypeCode_ENUM:
			return "", ErrFallthrough
		}
		if !isScalarFastPathTypeCode(code) {
			return "", ErrFallthrough
		}
		if IsNull(value) {
			return formatter.GetNullString(), nil
		}
		return formatGCVScalarLiteral(q, value)
	}
}

// FormatSpannerCLIValue is a [FormatComplexFunc] that formats scalars for
// [SpannerCLICompatibleFormatConfig]. It returns [ErrFallthrough] for ARRAY,
// STRUCT, and type codes outside the supported scalar set.
func FormatSpannerCLIValue(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	code, ok := scalarTypeCode(value)
	if !ok {
		return "", ErrFallthrough
	}
	switch code {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT:
		return "", ErrFallthrough
	}
	if !isScalarFastPathTypeCode(code) {
		return "", ErrFallthrough
	}
	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	return formatGCVScalarSpannerCLI(value)
}

func formatGCVScalarSimple(gcv spanner.GenericColumnValue) (string, error) {
	if err := validateScalarWire(gcv); err != nil {
		return "", err
	}
	switch gcv.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return strconv.FormatBool(gcv.Value.GetBoolValue()), nil
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM, sppb.TypeCode_STRING,
		sppb.TypeCode_TIMESTAMP, sppb.TypeCode_DATE, sppb.TypeCode_JSON,
		sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_FLOAT32:
		return formatFloatSimple(gcv, 32)
	case sppb.TypeCode_FLOAT64:
		return formatFloatSimple(gcv, 64)
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return internal.ReadableStringFromBase64Wire(gcv.Value.GetStringValue())
	case sppb.TypeCode_NUMERIC:
		return numericWireString(gcv), nil
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("%w: %v", ErrUnknownType, gcv.Type.String())
	}
}

func formatGCVScalarLiteral(q LiteralQuoteConfig, gcv spanner.GenericColumnValue) (string, error) {
	if err := validateScalarWire(gcv); err != nil {
		return "", err
	}
	policy := toInternalQuotePolicy(q)
	switch gcv.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return strconv.FormatBool(gcv.Value.GetBoolValue()), nil
	case sppb.TypeCode_INT64:
		s := gcv.Value.GetStringValue()
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			return "", err
		}
		return s, nil
	case sppb.TypeCode_FLOAT32:
		f, err := gcvFloat32(gcv.Value)
		if err != nil {
			return "", err
		}
		return internal.Float32ToLiteralPolicy(f, policy), nil
	case sppb.TypeCode_FLOAT64:
		f, err := gcvFloat64(gcv.Value)
		if err != nil {
			return "", err
		}
		return internal.Float64ToLiteralPolicy(f, policy), nil
	case sppb.TypeCode_STRING:
		return internal.ToStringLiteralPolicy(gcv.Value.GetStringValue(), policy), nil
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		b, err := internal.DecodeBase64Wire(gcv.Value.GetStringValue())
		if err != nil {
			return "", err
		}
		return internal.ToReadableBytesLiteralPolicy(b, policy), nil
	case sppb.TypeCode_TIMESTAMP:
		return stringBasedLiteral("TIMESTAMP", gcv.Value.GetStringValue(), policy), nil
	case sppb.TypeCode_DATE:
		return stringBasedLiteral("DATE", gcv.Value.GetStringValue(), policy), nil
	case sppb.TypeCode_NUMERIC:
		return stringBasedLiteral("NUMERIC", numericWireString(gcv), policy), nil
	case sppb.TypeCode_JSON:
		s := gcv.Value.GetStringValue()
		return stringBasedLiteral("JSON", s, policy), nil
	case sppb.TypeCode_INTERVAL:
		return stringLiteralCast("INTERVAL", gcv.Value.GetStringValue(), policy), nil
	case sppb.TypeCode_UUID:
		return stringLiteralCast("UUID", gcv.Value.GetStringValue(), policy), nil
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("%w: %v", ErrUnknownType, gcv.Type.String())
	}
}

func formatGCVScalarSpannerCLI(gcv spanner.GenericColumnValue) (string, error) {
	if err := validateScalarWire(gcv); err != nil {
		return "", err
	}
	switch gcv.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return strconv.FormatBool(gcv.Value.GetBoolValue()), nil
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM, sppb.TypeCode_STRING,
		sppb.TypeCode_BYTES, sppb.TypeCode_PROTO,
		sppb.TypeCode_TIMESTAMP, sppb.TypeCode_DATE,
		sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_FLOAT32:
		return formatFloatSpannerCLI(gcv, 32)
	case sppb.TypeCode_FLOAT64:
		return formatFloatSpannerCLI(gcv, 64)
	case sppb.TypeCode_NUMERIC:
		return trimSpannerCLINumericFraction(numericWireString(gcv)), nil
	case sppb.TypeCode_JSON:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("%w: %v", ErrUnknownType, gcv.Type.String())
	}
}

func formatFloatSimple(gcv spanner.GenericColumnValue, bits int) (string, error) {
	if bits == 32 {
		f, err := gcvFloat32(gcv.Value)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(float64(f), 'g', -1, 32), nil
	}
	f, err := gcvFloat64(gcv.Value)
	if err != nil {
		return "", err
	}
	return strconv.FormatFloat(f, 'g', -1, 64), nil
}

func formatFloatSpannerCLI(gcv spanner.GenericColumnValue, bits int) (string, error) {
	if bits == 32 {
		f, err := gcvFloat32(gcv.Value)
		if err != nil {
			return "", err
		}
		return formatSpannerCLIFloat(float64(f), 32), nil
	}
	f, err := gcvFloat64(gcv.Value)
	if err != nil {
		return "", err
	}
	return formatSpannerCLIFloat(f, 64), nil
}

// formatSpannerCLIFloat matches spanner-cli: integral finite values omit the
// fractional part; non-integral values use six digits after the decimal.
func formatSpannerCLIFloat(f float64, bits int) string {
	if isFiniteIntegralFloat(f) {
		return strconv.FormatFloat(f, 'f', 0, bits)
	}
	return strconv.FormatFloat(f, 'f', 6, bits)
}

func isFiniteIntegralFloat(f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	return f == math.Trunc(f)
}

// gcvFloat64 reads a FLOAT64 wire value (NumberValue or NaN/±Inf strings).
// Logic matches cloud.google.com/go/spanner getFloat64Value.
func gcvFloat64(v *structpb.Value) (float64, error) {
	switch x := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return x.NumberValue, nil
	case *structpb.Value_StringValue:
		switch x.StringValue {
		case "NaN":
			return math.NaN(), nil
		case "Infinity":
			return math.Inf(1), nil
		case "-Infinity":
			return math.Inf(-1), nil
		default:
			return 0, fmt.Errorf("%w: FLOAT64 unexpected float string %q", ErrMalformedWire, x.StringValue)
		}
	default:
		return 0, fmt.Errorf("%w: FLOAT64 value kind %T", ErrMalformedWire, v.GetKind())
	}
}

// gcvFloat32 reads a FLOAT32 wire value. Logic matches cloud.google.com/go/spanner getFloat32Value.
func gcvFloat32(v *structpb.Value) (float32, error) {
	switch x := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return float32(x.NumberValue), nil
	case *structpb.Value_StringValue:
		switch x.StringValue {
		case "NaN":
			return float32(math.NaN()), nil
		case "Infinity":
			return float32(math.Inf(1)), nil
		case "-Infinity":
			return float32(math.Inf(-1)), nil
		default:
			return 0, fmt.Errorf("%w: FLOAT32 unexpected float string %q", ErrMalformedWire, x.StringValue)
		}
	default:
		return 0, fmt.Errorf("%w: FLOAT32 value kind %T", ErrMalformedWire, v.GetKind())
	}
}

// numericWireString returns the NUMERIC string wire payload as-is. Spanner, Spanner Omni, and the
// emulator already emit canonical decimal strings; [gcvctor.NumericValue] and friends store the
// same shape. Normalizing via big.Rat is the constructor's job, not spanvalue formatting.
func numericWireString(gcv spanner.GenericColumnValue) string {
	return gcv.Value.GetStringValue()
}
