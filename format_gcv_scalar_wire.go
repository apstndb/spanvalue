package spanvalue

import (
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// isScalarFastPathTypeCode reports whether the preset scalar plugins format this
// [sppb.TypeCode] directly. Other codes fall through to later plugins or
// [FormatConfig.formatSimpleColumn].
func isScalarFastPathTypeCode(code sppb.TypeCode) bool {
	switch code {
	case sppb.TypeCode_BOOL, sppb.TypeCode_INT64, sppb.TypeCode_ENUM,
		sppb.TypeCode_FLOAT32, sppb.TypeCode_FLOAT64,
		sppb.TypeCode_STRING, sppb.TypeCode_BYTES, sppb.TypeCode_PROTO,
		sppb.TypeCode_TIMESTAMP, sppb.TypeCode_DATE, sppb.TypeCode_NUMERIC,
		sppb.TypeCode_JSON, sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
		return true
	default:
		return false
	}
}

func validateScalarWire(gcv spanner.GenericColumnValue) error {
	if IsNull(gcv) {
		return fmt.Errorf("%w: null value", ErrUnknownType)
	}
	code := gcv.Type.GetCode()
	switch code {
	case sppb.TypeCode_BOOL:
		return requireBoolWire(gcv.Value, code)
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM, sppb.TypeCode_STRING,
		sppb.TypeCode_BYTES, sppb.TypeCode_PROTO, sppb.TypeCode_TIMESTAMP, sppb.TypeCode_DATE,
		sppb.TypeCode_NUMERIC, sppb.TypeCode_INTERVAL, sppb.TypeCode_UUID:
		return requireStringWire(gcv.Value, code)
	case sppb.TypeCode_FLOAT32, sppb.TypeCode_FLOAT64:
		return validateFloatWire(gcv.Value, code)
	case sppb.TypeCode_JSON:
		// [sppb.TypeCode_JSON] and PG_JSONB-annotated JSON are encoded as a JSON string
		// (see TypeCode godoc in cloud.google.com/go/spanner/apiv1/spannerpb).
		return requireStringWire(gcv.Value, code)
	default:
		return fmt.Errorf("%w: %v", ErrUnknownType, gcv.Type.String())
	}
}

func requireBoolWire(v *structpb.Value, code sppb.TypeCode) error {
	if _, ok := v.GetKind().(*structpb.Value_BoolValue); !ok {
		return fmt.Errorf("%w: %v value kind %T", ErrUnknownType, code, v.GetKind())
	}
	return nil
}

func requireStringWire(v *structpb.Value, code sppb.TypeCode) error {
	if _, ok := v.GetKind().(*structpb.Value_StringValue); !ok {
		return fmt.Errorf("%w: %v value kind %T", ErrUnknownType, code, v.GetKind())
	}
	return nil
}

func validateFloatWire(v *structpb.Value, code sppb.TypeCode) error {
	// TypeCode_FLOAT32/FLOAT64: JSON number, or "NaN"/"Infinity"/"-Infinity" strings.
	// Spanner client wire uses structpb NumberValue for finite values and StringValue
	// for non-finite values (see [gcvctor.float64ToStructpbValue]).
	switch k := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return nil
	case *structpb.Value_StringValue:
		switch k.StringValue {
		case "NaN", "Infinity", "-Infinity":
			return nil
		default:
			return fmt.Errorf("%w: %v unexpected float string %q", ErrUnknownType, code, k.StringValue)
		}
	default:
		return fmt.Errorf("%w: %v value kind %T", ErrUnknownType, code, v.GetKind())
	}
}
