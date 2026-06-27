package gcvctor_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/gcvctor"
)

func TestWithType(t *testing.T) {
	t.Parallel()

	src := spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue("1"),
	}
	destType := typector.CodeToSimpleType(sppb.TypeCode_INT64)

	got := gcvctor.WithType(destType, src)
	want := spanner.GenericColumnValue{
		Type:  destType,
		Value: structpb.NewStringValue("1"),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestWithTypeNilDestinationNormalizesUnspecified(t *testing.T) {
	t.Parallel()

	src := gcvctor.Int64Value(1)
	got := gcvctor.WithType(nil, src)
	wantType := typector.CodeToSimpleType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED)
	if got.Type.GetCode() != wantType.GetCode() {
		t.Fatalf("Type code = %v, want %v", got.Type.GetCode(), wantType.GetCode())
	}
	if diff := cmp.Diff(src.Value, got.Value, protocmp.Transform()); diff != "" {
		t.Fatalf("Value changed (-want +got):\n%s", diff)
	}
}

func TestWithEquivalentTypeScalar(t *testing.T) {
	t.Parallel()

	src := gcvctor.Int64Value(42)
	destType := typector.CodeToSimpleType(sppb.TypeCode_INT64)

	got, err := gcvctor.WithEquivalentType(destType, src)
	if err != nil {
		t.Fatalf("WithEquivalentType: %v", err)
	}
	want := spanner.GenericColumnValue{Type: destType, Value: src.Value}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestWithEquivalentTypeArray(t *testing.T) {
	t.Parallel()

	elemType := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	src, err := gcvctor.ArrayValueOf(elemType, gcvctor.Int64Value(1), gcvctor.Int64Value(2))
	if err != nil {
		t.Fatalf("ArrayValueOf: %v", err)
	}
	destArrayType := typector.ElemTypeToArrayType(elemType)

	got, err := gcvctor.WithEquivalentType(destArrayType, src)
	if err != nil {
		t.Fatalf("WithEquivalentType: %v", err)
	}
	want := spanner.GenericColumnValue{Type: destArrayType, Value: src.Value}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestWithEquivalentTypeStructIgnoresFieldNames(t *testing.T) {
	t.Parallel()

	fieldType := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	srcType := typector.NameTypeToStructType("a", fieldType)
	destType := typector.NameTypeToStructType("b", fieldType)
	src, err := gcvctor.StructValueOf([]string{"a"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1)})
	if err != nil {
		t.Fatalf("StructValueOf: %v", err)
	}
	src.Type = srcType

	got, err := gcvctor.WithEquivalentType(destType, src)
	if err != nil {
		t.Fatalf("WithEquivalentType: %v", err)
	}
	want := spanner.GenericColumnValue{Type: destType, Value: src.Value}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestWithEquivalentTypeMismatch(t *testing.T) {
	t.Parallel()

	src := gcvctor.Int64Value(1)
	destType := typector.CodeToSimpleType(sppb.TypeCode_STRING)

	_, err := gcvctor.WithEquivalentType(destType, src)
	if !errors.Is(err, gcvctor.ErrTypeMismatch) {
		t.Fatalf("error = %v, want ErrTypeMismatch", err)
	}
}

func TestWithEquivalentTypeNilDestination(t *testing.T) {
	t.Parallel()

	_, err := gcvctor.WithEquivalentType(nil, gcvctor.Int64Value(1))
	if !errors.Is(err, gcvctor.ErrNilDestinationType) {
		t.Fatalf("error = %v, want ErrNilDestinationType", err)
	}
}

func TestWithExactTypeMatch(t *testing.T) {
	t.Parallel()

	destType := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	src := spanner.GenericColumnValue{Type: destType, Value: structpb.NewStringValue("7")}

	got, err := gcvctor.WithExactType(destType, src)
	if err != nil {
		t.Fatalf("WithExactType: %v", err)
	}
	want := spanner.GenericColumnValue{Type: destType, Value: src.Value}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestWithExactTypeStructFieldNamesDiffer(t *testing.T) {
	t.Parallel()

	fieldType := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	srcType := typector.NameTypeToStructType("a", fieldType)
	destType := typector.NameTypeToStructType("b", fieldType)
	src, err := gcvctor.StructValueOf([]string{"a"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1)})
	if err != nil {
		t.Fatalf("StructValueOf: %v", err)
	}
	src.Type = srcType

	_, err = gcvctor.WithExactType(destType, src)
	if !errors.Is(err, gcvctor.ErrTypeMismatch) {
		t.Fatalf("error = %v, want ErrTypeMismatch", err)
	}
}

func TestWithExactTypeNilDestination(t *testing.T) {
	t.Parallel()

	_, err := gcvctor.WithExactType(nil, gcvctor.Int64Value(1))
	if !errors.Is(err, gcvctor.ErrNilDestinationType) {
		t.Fatalf("error = %v, want ErrNilDestinationType", err)
	}
}
