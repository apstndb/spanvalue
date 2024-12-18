package valuector_test

import (
	"encoding/base64"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/valuector"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestParseExpr(t *testing.T) {
	tests := []struct {
		desc  string
		input spanner.GenericColumnValue
		want  spanner.GenericColumnValue
	}{
		{
			"NULL",
			valuector.TypedNull(sppb.TypeCode_INT64),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewNullValue(),
			},
		},
		{
			"TRUE",
			valuector.BoolValue(true),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
				Value: structpb.NewBoolValue(true),
			},
		},
		{
			`FALSE`,
			valuector.BoolValue(false),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
				Value: structpb.NewBoolValue(false),
			},
		},
		{
			"1",
			valuector.Int64Value(1),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("1"),
			},
		},
		{
			`3.14`,
			valuector.Float64Value(3.14),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewNumberValue(3.14),
			},
		},
		{
			`"foo"`,
			valuector.StringValue("foo"),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
				Value: structpb.NewStringValue("foo"),
			},
		},
		{
			`b"foo"`,
			valuector.BytesValue([]byte("foo")),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BYTES),
				Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString([]byte("foo"))),
			},
		},
		{
			`DATE "1970-01-01"`,
			valuector.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1}),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_DATE),
				Value: structpb.NewStringValue("1970-01-01"),
			},
		},
		{
			`TIMESTAMP "1970-01-01T00:00:00Z"`,
			valuector.TimestampValue(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("1970-01-01T00:00:00Z"),
			},
		},
		// {`NUMERIC "3.14"`, valuector.NumericValue(big.NewRat(314, 100))},

		// Note: Usually, JSON representation is not stable.
		{
			`JSON '{"foo":"bar"}'`,
			must(valuector.JSONValue(map[string]string{"foo": "bar"})),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_JSON),
				Value: structpb.NewStringValue(`{"foo":"bar"}`),
			},
		},
		{
			`[1, 2, 3]`,
			must(valuector.ArrayValue(valuector.Int64Value(1), valuector.Int64Value(2), valuector.Int64Value(3))),
			spanner.GenericColumnValue{
				Type: typector.ElemTypeToArrayType(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("2"),
							structpb.NewStringValue("3"),
						},
					},
				),
			},
		},
		{
			`(1, "foo", 3.14)`,
			must(valuector.StructValue(
				[]string{"", "", ""},
				[]spanner.GenericColumnValue{valuector.Int64Value(1), valuector.StringValue("foo"), valuector.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: typector.StructTypeFieldsToStructType([]*sppb.StructType_Field{
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_INT64),
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_STRING),
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_FLOAT64),
				}),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			`STRUCT(1 AS int64_value, "foo" AS string_value, 3.14 AS float64_value)`,
			must(valuector.StructValue(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{valuector.Int64Value(1), valuector.StringValue("foo"), valuector.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: must(typector.NameCodeSlicesToStructType(
					[]string{"int64_value", "string_value", "float64_value"},
					[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING, sppb.TypeCode_FLOAT64},
				)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			`STRUCT<int64_value INT64, string_value STRING, float64_value FLOAT64>(1, "foo", 3.14)`,
			must(valuector.StructValue(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{valuector.Int64Value(1), valuector.StringValue("foo"), valuector.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: must(typector.NameCodeSlicesToStructType(
					[]string{"int64_value", "string_value", "float64_value"},
					[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING, sppb.TypeCode_FLOAT64},
				)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			"(1)",
			valuector.Int64Value(1),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("1"),
			},
		},
		{
			"PENDING_COMMIT_TIMESTAMP()",
			valuector.StringBasedValue(sppb.TypeCode_TIMESTAMP, "spanner.commit_timestamp()"),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("spanner.commit_timestamp()"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.input, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
