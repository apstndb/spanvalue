package valuector

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/apstndb/spantype"
	gocmp "github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"google.golang.org/protobuf/types/known/structpb"
)

func BoolValue(v bool) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
		Value: structpb.NewBoolValue(v),
	}
}

func Int64Value(v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

func Float64Value(v float64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
		Value: structpb.NewNumberValue(v),
	}
}

func StringValue(v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
		Value: structpb.NewStringValue(v),
	}
}

func BytesValue(v []byte) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BYTES),
		Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(v)),
	}
}

func StringBasedValue(code sppb.TypeCode, v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewStringValue(v),
	}
}

func DateValue(v civil.Date) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_DATE, v.String())
}

func TimestampValue(v time.Time) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_TIMESTAMP, v.Format(time.RFC3339Nano))
}

func NumericValue(v *big.Rat) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_NUMERIC, spanner.NumericString(v))
}

func JSONValue(v any) (spanner.GenericColumnValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return StringBasedValue(sppb.TypeCode_JSON, string(b)), nil
}

// ArrayValue constructs ARRAY GenericColumnValue.
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func ArrayValue(vs ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(vs) == 0 {
		return TypedNull(sppb.TypeCode_INT64), nil
	}

	typ := vs[0].Type
	var values []*structpb.Value
	for i, v := range vs {
		if !gocmp.Equal(typ, v.Type, protocmp.Transform()) {
			return spanner.GenericColumnValue{}, fmt.Errorf("%v is not %v", spantype.FormatTypeMoreVerbose(vs[i].Type), spantype.FormatTypeMoreVerbose(typ))
		}
		values = append(values, v.Value)
	}

	return spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(typ),
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}, nil
}

// StructValue constructs STRUCT GenericColumnValue.
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func StructValue(names []string, gcvs []spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(names) != len(gcvs) {
		return spanner.GenericColumnValue{}, fmt.Errorf("len(names)=%v != len(gcvs)=%v", len(names), len(gcvs))
	}

	var types []*sppb.Type
	var values []*structpb.Value
	for _, gcv := range gcvs {
		types = append(types, gcv.Type)
		values = append(values, gcv.Value)
	}

	typ, err := typector.NameTypeSlicesToStructType(names, types)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}

	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}, nil
}

func TypedNull(code sppb.TypeCode) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewNullValue(),
	}
}
