package spanvalue

import (
	"encoding/base64"
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
)

var presetScalarPlugins = []FormatComplexFunc{
	FormatSimpleValue,
	FormatLiteralValue,
	FormatSpannerCLIValue,
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
// removed so scalars use Decode and [FormatNullable].
func FormatConfigWithoutScalarPlugins(fc *FormatConfig) *FormatConfig {
	if fc == nil {
		return nil
	}
	clone := fc.Clone()
	clone.FormatComplexPlugins = slices.DeleteFunc(clone.FormatComplexPlugins, isPresetScalarPlugin)
	return clone
}

func scalarFastPathActive(formatter Formatter, presetNullable FormatNullableFunc) bool {
	fc, ok := formatter.(*FormatConfig)
	if !ok {
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
	switch value.Type.GetCode() {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT:
		return "", ErrFallthrough
	}
	if !isScalarFastPathTypeCode(value.Type.GetCode()) {
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
	return formatGCVScalarLiteral(value)
}

// FormatSpannerCLIValue is a [FormatComplexFunc] for [SpannerCLICompatibleFormatConfig].
func FormatSpannerCLIValue(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	switch value.Type.GetCode() {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT:
		return "", ErrFallthrough
	}
	if !isScalarFastPathTypeCode(value.Type.GetCode()) {
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
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_FLOAT32:
		return formatFloatSimple(gcv, 32)
	case sppb.TypeCode_FLOAT64:
		return formatFloatSimple(gcv, 64)
	case sppb.TypeCode_STRING:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return readableStringFromBytesWire(gcv)
	case sppb.TypeCode_TIMESTAMP:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_DATE:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_NUMERIC:
		return numericWireString(gcv), nil
	case sppb.TypeCode_JSON:
		if gcv.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return pgJSONBString(gcv)
		}
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("%w: %v", ErrUnknownType, gcv.Type.String())
	}
}

func formatGCVScalarLiteral(gcv spanner.GenericColumnValue) (string, error) {
	if err := validateScalarWire(gcv); err != nil {
		return "", err
	}
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
		return internal.Float32ToLiteral(f), nil
	case sppb.TypeCode_FLOAT64:
		f, err := gcvFloat64(gcv.Value)
		if err != nil {
			return "", err
		}
		return internal.Float64ToLiteral(f), nil
	case sppb.TypeCode_STRING:
		return internal.ToStringLiteral(gcv.Value.GetStringValue()), nil
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		b, err := bytesFromGCVString(gcv)
		if err != nil {
			return "", err
		}
		return internal.ToReadableBytesLiteral(b), nil
	case sppb.TypeCode_TIMESTAMP:
		return stringBasedLiteral("TIMESTAMP", gcv.Value.GetStringValue()), nil
	case sppb.TypeCode_DATE:
		return stringBasedLiteral("DATE", gcv.Value.GetStringValue()), nil
	case sppb.TypeCode_NUMERIC:
		return stringBasedLiteral("NUMERIC", numericWireString(gcv)), nil
	case sppb.TypeCode_JSON:
		if gcv.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			s, err := pgJSONBString(gcv)
			if err != nil {
				return "", err
			}
			return stringBasedLiteral("JSON", s), nil
		}
		return stringBasedLiteral("JSON", gcv.Value.GetStringValue()), nil
	case sppb.TypeCode_INTERVAL:
		return stringLiteralCast("INTERVAL", gcv.Value.GetStringValue()), nil
	case sppb.TypeCode_UUID:
		return stringLiteralCast("UUID", gcv.Value.GetStringValue()), nil
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
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_FLOAT32:
		return formatFloatSpannerCLI(gcv, 32)
	case sppb.TypeCode_FLOAT64:
		return formatFloatSpannerCLI(gcv, 64)
	case sppb.TypeCode_STRING:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		// Wire is base64; Spanner CLI output is the same encoding.
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_TIMESTAMP:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_DATE:
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_NUMERIC:
		return trimSpannerCLINumericFraction(numericWireString(gcv)), nil
	case sppb.TypeCode_JSON:
		if gcv.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return pgJSONBString(gcv)
		}
		return gcv.Value.GetStringValue(), nil
	case sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
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
		return fmt.Sprintf("%v", f), nil
	}
	f, err := gcvFloat64(gcv.Value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", f), nil
}

func formatFloatSpannerCLI(gcv spanner.GenericColumnValue, bits int) (string, error) {
	if bits == 32 {
		f, err := gcvFloat32(gcv.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%f", f), nil
	}
	f, err := gcvFloat64(gcv.Value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%f", f), nil
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

func readableStringFromBytesWire(gcv spanner.GenericColumnValue) (string, error) {
	return internal.ReadableStringFromBase64Wire(gcv.Value.GetStringValue())
}

func bytesFromGCVString(gcv spanner.GenericColumnValue) ([]byte, error) {
	return base64.StdEncoding.DecodeString(gcv.Value.GetStringValue())
}

// numericWireString returns the NUMERIC string wire payload as-is. Spanner, Spanner Omni, and the
// emulator already emit canonical decimal strings; [gcvctor.NumericValue] and friends store the
// same shape. Normalizing via big.Rat is the constructor's job, not spanvalue formatting.
func numericWireString(gcv spanner.GenericColumnValue) string {
	return gcv.Value.GetStringValue()
}

func pgJSONBString(gcv spanner.GenericColumnValue) (string, error) {
	return gcv.Value.GetStringValue(), nil
}
