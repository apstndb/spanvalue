package gcvctor_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

func expectPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func TestMustStructValueOf_panicsOnMismatchedCounts(t *testing.T) {
	t.Parallel()

	expectPanic(t, func() {
		gcvctor.MustStructValueOf(
			[]string{"a"},
			[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.Int64Value(2)},
		)
	})
}

func TestMustArrayValueOf_panicsOnTypeMismatch(t *testing.T) {
	t.Parallel()

	elemType := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	expectPanic(t, func() {
		gcvctor.MustArrayValueOf(elemType, gcvctor.StringValue("x"))
	})
}

func TestMustNormalizeArrayElements_panicsOnNilElementType(t *testing.T) {
	t.Parallel()

	expectPanic(t, func() {
		gcvctor.MustNormalizeArrayElements(nil, gcvctor.Int64Value(1))
	})
}

func TestMustDateStringValue_panicsOnInvalidInput(t *testing.T) {
	t.Parallel()

	expectPanic(t, func() {
		gcvctor.MustDateStringValue("not-a-date")
	})
}

func TestMustJSONValue_panicsOnMarshalError(t *testing.T) {
	t.Parallel()

	expectPanic(t, func() {
		gcvctor.MustJSONValue(make(chan int))
	})
}

func TestMustPGJSONBValue_panicsOnMarshalError(t *testing.T) {
	t.Parallel()

	expectPanic(t, func() {
		gcvctor.MustPGJSONBValue(make(chan int))
	})
}

func TestMustStructValueOf_matchesStructValueOf(t *testing.T) {
	t.Parallel()

	names := []string{"id", "name"}
	gcvs := []spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo")}
	want, err := gcvctor.StructValueOf(names, gcvs)
	if err != nil {
		t.Fatalf("StructValueOf: %v", err)
	}
	got := gcvctor.MustStructValueOf(names, gcvs)
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("MustStructValueOf mismatch (-want +got):\n%s", diff)
	}
}

func TestMustArrayValueOf_nestedStruct(t *testing.T) {
	t.Parallel()

	structType, err := typector.NameCodeSlicesToStructType(
		[]string{"Code", "DisplayOrder"},
		[]sppb.TypeCode{sppb.TypeCode_STRING, sppb.TypeCode_INT64},
	)
	if err != nil {
		t.Fatalf("NameCodeSlicesToStructType: %v", err)
	}

	arrayParam := gcvctor.MustArrayValueOf(structType,
		gcvctor.MustStructValueOf(
			[]string{"Code", "DisplayOrder"},
			[]spanner.GenericColumnValue{gcvctor.StringValue("10"), gcvctor.Int64Value(1)},
		),
		gcvctor.NullOf(structType),
	)

	if arrayParam.Type.Code != sppb.TypeCode_ARRAY {
		t.Fatalf("Type.Code = %v, want ARRAY", arrayParam.Type.Code)
	}
	values := arrayParam.Value.GetListValue().Values
	if len(values) != 2 {
		t.Fatalf("len(values) = %d, want 2", len(values))
	}
	if values[0].GetListValue() == nil {
		t.Fatal("first element: expected non-null struct list value")
	}
	nullElem := spanner.GenericColumnValue{Type: structType, Value: values[1]}
	if !spanvalue.IsNull(nullElem) {
		t.Fatal("second element: expected SQL NULL")
	}
}

func TestMustNormalizeArrayElements_matchesNormalizeArrayElements(t *testing.T) {
	t.Parallel()

	elemType := typector.CodeToSimpleType(sppb.TypeCode_DATE)
	elems := []spanner.GenericColumnValue{
		gcvctor.MustDateStringValue("2026-04-01"),
		gcvctor.NullOf(nil),
	}
	want, err := gcvctor.NormalizeArrayElements(elemType, elems...)
	if err != nil {
		t.Fatalf("NormalizeArrayElements: %v", err)
	}
	got := gcvctor.MustNormalizeArrayElements(elemType, elems...)
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("MustNormalizeArrayElements mismatch (-want +got):\n%s", diff)
	}
}

func TestMustDateStringValue_wrapsParseError(t *testing.T) {
	t.Parallel()

	var panicked any
	func() {
		defer func() { panicked = recover() }()
		gcvctor.MustDateStringValue("bad")
	}()
	if panicked == nil {
		t.Fatal("expected panic")
	}
	if _, ok := panicked.(error); !ok {
		t.Fatalf("panic value type = %T, want error", panicked)
	}
}

func TestMustStructValueOf_panicsPreserveErrMismatchedCounts(t *testing.T) {
	t.Parallel()

	var panicked any
	func() {
		defer func() { panicked = recover() }()
		gcvctor.MustStructValueOf([]string{"a"}, nil)
	}()
	if panicked == nil {
		t.Fatal("expected panic")
	}
	err, ok := panicked.(error)
	if !ok {
		t.Fatalf("panic value type = %T, want error", panicked)
	}
	if !errors.Is(err, gcvctor.ErrMismatchedCounts) {
		t.Fatalf("panic = %v, want ErrMismatchedCounts", panicked)
	}
}
