package gcvctor_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCompositeNullTransport(t *testing.T) {
	t.Parallel()
	typ := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	for _, kind := range []string{"array", "struct"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			explicit := structpb.NewNullValue()
			scalar := structpb.NewStringValue("7")
			inputs := []spanner.GenericColumnValue{{Type: typ}, {Type: typ}, {Type: typ, Value: explicit}, {Type: typ, Value: scalar}}
			var got spanner.GenericColumnValue
			var err error
			var wantType *sppb.Type
			if kind == "array" {
				got, err = gcvctor.ArrayValueOf(typ, inputs...)
				wantType = typector.ElemTypeToArrayType(typ)
			} else {
				got, err = gcvctor.StructValueOf([]string{"a", "b", "c", "d"}, inputs)
				wantType = &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "a", Type: typ}, {Name: "b", Type: typ}, {Name: "c", Type: typ}, {Name: "d", Type: typ}}}}
			}
			if err != nil {
				t.Fatal(err)
			}
			wantValue := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNullValue(), structpb.NewNullValue(), structpb.NewNullValue(), structpb.NewStringValue("7")}})
			want := spanner.GenericColumnValue{Type: wantType, Value: wantValue}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("constructor (-want +got):\n%s", diff)
			}
			elems := got.Value.GetListValue().GetValues()
			if elems[0] == elems[1] || elems[0] == explicit || elems[1] == explicit {
				t.Error("nil Values must each receive a fresh NULL")
			}
			if elems[2] != explicit || elems[3] != scalar {
				t.Error("non-nil Values must be borrowed")
			}
			if inputs[0].Value != nil || inputs[1].Value != nil {
				t.Error("constructor mutated input")
			}
			for _, transport := range []struct {
				name      string
				marshal   func(proto.Message) ([]byte, error)
				unmarshal func([]byte, proto.Message) error
			}{{"protobuf", proto.Marshal, proto.Unmarshal}, {"JSON", protojson.Marshal, protojson.Unmarshal}} {
				t.Run(transport.name, func(t *testing.T) {
					t.Parallel()
					wire, err := transport.marshal(got.Value)
					if err != nil {
						t.Fatal(err)
					}
					decoded := new(structpb.Value)
					if err := transport.unmarshal(wire, decoded); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(wantValue, decoded, protocmp.Transform()); diff != "" {
						t.Errorf("round trip (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestCompositeNilValueStillRequiresType(t *testing.T) {
	t.Parallel()
	typ := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	if _, err := gcvctor.ArrayValueOf(typ, spanner.GenericColumnValue{}); !errors.Is(err, gcvctor.ErrNilElementType) {
		t.Errorf("nil array type: %v", err)
	}
	if _, err := gcvctor.ArrayValueOf(typ, spanner.GenericColumnValue{Type: typector.CodeToSimpleType(sppb.TypeCode_STRING)}); !errors.Is(err, gcvctor.ErrTypeMismatch) {
		t.Errorf("mismatched null type: %v", err)
	}
	if _, err := gcvctor.StructValueOf([]string{"x"}, []spanner.GenericColumnValue{{}}); !errors.Is(err, gcvctor.ErrNilFieldType) {
		t.Errorf("nil struct type: %v", err)
	}
}
