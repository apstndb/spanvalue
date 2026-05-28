package gcvctor_test

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFromPtrNullableScalars(t *testing.T) {
	t.Parallel()

	i := int64(42)
	s := "hello"
	f := 1.5
	b := true
	d := civil.Date{Year: 2026, Month: 5, Day: 28}
	ts := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	u := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	raw := []byte{1, 2}

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{"Int64FromPtr value", gcvctor.Int64FromPtr(&i), gcvctor.Int64Value(42)},
		{"Int64FromPtr null", gcvctor.Int64FromPtr(nil), nullFromCode(sppb.TypeCode_INT64)},
		{"StringFromPtr value", gcvctor.StringFromPtr(&s), gcvctor.StringValue("hello")},
		{"StringFromPtr null", gcvctor.StringFromPtr(nil), nullFromCode(sppb.TypeCode_STRING)},
		{"Float64FromPtr value", gcvctor.Float64FromPtr(&f), gcvctor.Float64Value(1.5)},
		{"BoolFromPtr value", gcvctor.BoolFromPtr(&b), gcvctor.BoolValue(true)},
		{"DateFromPtr value", gcvctor.DateFromPtr(&d), gcvctor.DateValue(d)},
		{"TimestampFromPtr value", gcvctor.TimestampFromPtr(&ts), gcvctor.TimestampValue(ts)},
		{"UUIDFromPtr value", gcvctor.UUIDFromPtr(&u), gcvctor.UUIDValue(u)},
		{"BytesFromPtr value", gcvctor.BytesFromPtr(&raw), gcvctor.BytesValue(raw)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, tt.got, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromNullableScalars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{
			name: "Int64FromNullable value",
			got:  gcvctor.Int64FromNullable(spanner.NullInt64{Int64: 7, Valid: true}),
			want: gcvctor.Int64Value(7),
		},
		{
			name: "Int64FromNullable null",
			got:  gcvctor.Int64FromNullable(spanner.NullInt64{}),
			want: nullFromCode(sppb.TypeCode_INT64),
		},
		{
			name: "StringFromNullable value",
			got:  gcvctor.StringFromNullable(spanner.NullString{StringVal: "x", Valid: true}),
			want: gcvctor.StringValue("x"),
		},
		{
			name: "StringFromNullable null",
			got:  gcvctor.StringFromNullable(spanner.NullString{}),
			want: nullFromCode(sppb.TypeCode_STRING),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, tt.got, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func nullFromCode(code sppb.TypeCode) spanner.GenericColumnValue {
	return gcvctor.NullFromCode(code)
}
