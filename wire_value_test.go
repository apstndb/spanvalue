package spanvalue

import (
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWireValue(t *testing.T) {
	t.Parallel()

	stringValue := structpb.NewStringValue("hello")
	listValue := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{structpb.NewBoolValue(true)},
	})
	explicitNull := structpb.NewNullValue()
	tests := []struct {
		name      string
		gcv       spanner.GenericColumnValue
		want      *structpb.Value
		wantAlias bool
	}{
		{
			name: "nil value",
			gcv:  spanner.GenericColumnValue{},
			want: structpb.NewNullValue(),
		},
		{
			name:      "explicit null",
			gcv:       spanner.GenericColumnValue{Value: explicitNull},
			want:      structpb.NewNullValue(),
			wantAlias: true,
		},
		{
			name:      "scalar",
			gcv:       spanner.GenericColumnValue{Value: stringValue},
			want:      structpb.NewStringValue("hello"),
			wantAlias: true,
		},
		{
			name:      "list",
			gcv:       spanner.GenericColumnValue{Value: listValue},
			want:      listValue,
			wantAlias: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := WireValue(tt.gcv)
			if got == nil {
				t.Fatal("WireValue() = nil, want a protobuf value")
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("WireValue() mismatch (-want +got):\n%s", diff)
			}
			if tt.wantAlias && got != tt.gcv.Value {
				t.Error("WireValue() did not return the input Value pointer")
			}
		})
	}

	first := WireValue(spanner.GenericColumnValue{})
	second := WireValue(spanner.GenericColumnValue{})
	if first == second {
		t.Error("WireValue() reused a protobuf NULL for distinct nil inputs")
	}
}

func TestWireValues(t *testing.T) {
	t.Parallel()

	stringValue := structpb.NewStringValue("hello")
	explicitNull := structpb.NewNullValue()
	gcvs := []spanner.GenericColumnValue{
		{},
		{Value: stringValue},
		{Value: explicitNull},
	}

	got := WireValues(gcvs)
	want := []*structpb.Value{
		structpb.NewNullValue(),
		structpb.NewStringValue("hello"),
		structpb.NewNullValue(),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("WireValues() mismatch (-want +got):\n%s", diff)
	}
	if got[1] != stringValue || got[2] != explicitNull {
		t.Error("WireValues() did not borrow non-nil input Value pointers")
	}

	got[1] = structpb.NewBoolValue(true)
	if gcvs[1].Value != stringValue {
		t.Error("mutating the result slice changed the input slice")
	}

	empty := WireValues(nil)
	if empty == nil || len(empty) != 0 {
		t.Errorf("WireValues(nil) = %#v, want a non-nil empty slice", empty)
	}
}
