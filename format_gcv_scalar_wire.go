package spanvalue

import (
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func validateScalarWire(gcv spanner.GenericColumnValue) error {
	if gcv.Value == nil {
		return nil
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
		if gcv.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return validatePGJSONBWire(gcv.Value)
		}
		return requireStringWire(gcv.Value, code)
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
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
	switch v.GetKind().(type) {
	case *structpb.Value_NumberValue, *structpb.Value_StringValue:
		return nil
	default:
		return fmt.Errorf("%w: %v value kind %T", ErrUnknownType, code, v.GetKind())
	}
}

func validatePGJSONBWire(v *structpb.Value) error {
	switch v.GetKind().(type) {
	case *structpb.Value_StringValue, *structpb.Value_StructValue, *structpb.Value_ListValue:
		return nil
	default:
		return fmt.Errorf("%w: JSON value kind %T", ErrUnknownType, v.GetKind())
	}
}
