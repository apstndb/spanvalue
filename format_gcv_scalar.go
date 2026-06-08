package spanvalue

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/internal"
)

var (
	_ FormatComplexFunc = FormatSimpleValue
	_ FormatComplexFunc = FormatLiteralValue
	_ FormatComplexFunc = FormatSpannerCLIValue
	_ FormatComplexFunc = FormatJSONSimpleValue
)

var presetScalarPlugins = []FormatComplexFunc{
	FormatSimpleValue,
	FormatLiteralValue,
	FormatSpannerCLIValue,
	FormatJSONSimpleValue,
}

func isPresetScalarPlugin(f FormatComplexFunc) bool {
	if f == nil {
		return false
	}
	fp := reflect.ValueOf(f).Pointer()
	for _, p := range presetScalarPlugins {
		if reflect.ValueOf(p).Pointer() == fp {
			return true
		}
	}
	return false
}

func nullableFuncsEqual(a, b FormatNullableFunc) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// FormatConfigWithoutScalarPlugins returns a clone of fc with preset scalar fast-path plugins
// removed so scalars use Decode and [FormatConfig.FormatNullable]. Set FormatNullable on the
// clone before formatting non-NULL scalars; nil FormatNullable returns [ErrFormatNullableRequired].
func FormatConfigWithoutScalarPlugins(fc *FormatConfig) *FormatConfig {
	if fc == nil {
		return nil
	}
	clone := fc.Clone()
	clone.FormatComplexPlugins = slices.DeleteFunc(clone.FormatComplexPlugins, isPresetScalarPlugin)
	return clone
}

// scalarTypeCode returns the column type code, or ok false to fall through when Type is nil.
func scalarTypeCode(value spanner.GenericColumnValue) (sppb.TypeCode, bool) {
	if value.Type == nil {
		return 0, false
	}
	return value.Type.GetCode(), true
}

func scalarFastPathActive(formatter Formatter, presetNullable FormatNullableFunc) bool {
	fc, ok := formatter.(*FormatConfig)
	if !ok {
		return true
	}
	if fc.FormatNullable == nil {
		return true
	}
	return nullableFuncsEqual(fc.FormatNullable, presetNullable)
}

// FormatSimpleValue is a [FormatComplexFunc] that formats non-ARRAY, non-STRUCT scalars for
// [SimpleFormatConfig] without constructing a [NullableValue]. It returns [ErrFallthrough] for
// ARRAY, STRUCT, and type codes outside [isScalarFastPathTypeCode], when
// [FormatConfig.FormatNullable] is not the preset default, or for types handled by earlier
// plugins. NUMERIC uses the string wire payload as-is; canonical wire is the GCV constructor's
// responsibility (see [github.com/apstndb/spanvalue/gcvctor.StringBasedValueFromCode]).
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
	if !scalarFastPathActive(formatter, formatNullableValueSimple) {
		return "", ErrFallthrough
	}
	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	return formatGCVScalarSimple(value)
}

// FormatLiteralValue is a [FormatComplexFunc] for [LiteralFormatConfig]. It returns
// [ErrFallthrough] for ARRAY, STRUCT, PROTO, and ENUM so [FormatProtoAsCast] and
// [FormatEnumAsCast] run first.
func FormatLiteralValue(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	switch value.Type.GetCode() {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT, sppb.TypeCode_PROTO, sppb.TypeCode_ENUM:
		return "", ErrFallthrough
	}
	if !isScalarFastPathTypeCode(value.Type.GetCode()) {
		return "", ErrFallthrough
	}
	if !scalarFastPathActive(formatter, formatNullableValueLiteral) {
		return "", ErrFallthrough
	}
	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	q := literalQuoteForFormatter(formatter)
	return formatGCVScalarLiteral(q, value)
}

// FormatSpannerCLIValue is a [FormatComplexFunc] for [SpannerCLICompatibleFormatConfig].
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
	if !scalarFastPathActive(formatter, FormatNullableSpannerCLICompatible) {
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
		return strconv.FormatFloat(float64(f), 'f', 6, 32), nil
	}
	f, err := gcvFloat64(gcv.Value)
	if err != nil {
		return "", err
	}
	return strconv.FormatFloat(f, 'f', 6, 64), nil
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
			return 0, fmt.Errorf("%w: unexpected FLOAT64 string %q", ErrUnknownType, x.StringValue)
		}
	default:
		return 0, fmt.Errorf("%w: FLOAT64 value kind %T", ErrUnknownType, v.GetKind())
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
			return 0, fmt.Errorf("%w: unexpected FLOAT32 string %q", ErrUnknownType, x.StringValue)
		}
	default:
		return 0, fmt.Errorf("%w: FLOAT32 value kind %T", ErrUnknownType, v.GetKind())
	}
}

// numericWireString returns the NUMERIC string wire payload as-is. Spanner, Spanner Omni, and the
// emulator already emit canonical decimal strings; [gcvctor.NumericValue] and friends store the
// same shape. Normalizing via big.Rat is the constructor's job, not spanvalue formatting.
func numericWireString(gcv spanner.GenericColumnValue) string {
	return gcv.Value.GetStringValue()
}
