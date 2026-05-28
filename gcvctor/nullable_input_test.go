package gcvctor_test

import (
	"encoding/base64"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func wantNull(code sppb.TypeCode) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewNullValue(),
	}
}

func wantBool(v bool) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
		Value: structpb.NewBoolValue(v),
	}
}

func wantInt64(v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

func wantFloat64(v float64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
		Value: structpb.NewNumberValue(v),
	}
}

func wantFloat32(v float32) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
		Value: structpb.NewNumberValue(float64(v)),
	}
}

func wantString(v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
		Value: structpb.NewStringValue(v),
	}
}

func wantBytes(v []byte) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BYTES),
		Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(v)),
	}
}

func wantDate(d civil.Date) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_DATE),
		Value: structpb.NewStringValue(d.String()),
	}
}

func wantTimestamp(ts time.Time) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
		Value: structpb.NewStringValue(ts.Format(time.RFC3339Nano)),
	}
}

func wantUUID(id uuid.UUID) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_UUID),
		Value: structpb.NewStringValue(id.String()),
	}
}

func TestFromPtrScalars(t *testing.T) {
	t.Parallel()

	b := true
	i := int64(42)
	f64 := 1.5
	f32 := float32(2.5)
	s := "hello"
	raw := []byte{1, 2}
	d := civil.Date{Year: 2026, Month: 5, Day: 28}
	ts := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	u := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{"BoolFromPtr value", gcvctor.BoolFromPtr(&b), wantBool(true)},
		{"BoolFromPtr null", gcvctor.BoolFromPtr(nil), wantNull(sppb.TypeCode_BOOL)},
		{"Int64FromPtr value", gcvctor.Int64FromPtr(&i), wantInt64(42)},
		{"Int64FromPtr null", gcvctor.Int64FromPtr(nil), wantNull(sppb.TypeCode_INT64)},
		{"Float64FromPtr value", gcvctor.Float64FromPtr(&f64), wantFloat64(1.5)},
		{"Float64FromPtr null", gcvctor.Float64FromPtr(nil), wantNull(sppb.TypeCode_FLOAT64)},
		{"Float32FromPtr value", gcvctor.Float32FromPtr(&f32), wantFloat32(2.5)},
		{"Float32FromPtr null", gcvctor.Float32FromPtr(nil), wantNull(sppb.TypeCode_FLOAT32)},
		{"StringFromPtr value", gcvctor.StringFromPtr(&s), wantString("hello")},
		{"StringFromPtr null", gcvctor.StringFromPtr(nil), wantNull(sppb.TypeCode_STRING)},
		{"BytesFromSlice value", gcvctor.BytesFromSlice(raw), wantBytes(raw)},
		{"BytesFromSlice null", gcvctor.BytesFromSlice(nil), wantNull(sppb.TypeCode_BYTES)},
		{"DateFromPtr value", gcvctor.DateFromPtr(&d), wantDate(d)},
		{"DateFromPtr null", gcvctor.DateFromPtr(nil), wantNull(sppb.TypeCode_DATE)},
		{"TimestampFromPtr value", gcvctor.TimestampFromPtr(&ts), wantTimestamp(ts)},
		{"TimestampFromPtr null", gcvctor.TimestampFromPtr(nil), wantNull(sppb.TypeCode_TIMESTAMP)},
		{"UUIDFromPtr value", gcvctor.UUIDFromPtr(&u), wantUUID(u)},
		{"UUIDFromPtr null", gcvctor.UUIDFromPtr(nil), wantNull(sppb.TypeCode_UUID)},
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

	d := civil.Date{Year: 2026, Month: 5, Day: 28}
	ts := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	u := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{"BoolFromNullable value", gcvctor.BoolFromNullable(spanner.NullBool{Bool: true, Valid: true}), wantBool(true)},
		{"BoolFromNullable null", gcvctor.BoolFromNullable(spanner.NullBool{}), wantNull(sppb.TypeCode_BOOL)},
		{"Int64FromNullable value", gcvctor.Int64FromNullable(spanner.NullInt64{Int64: 7, Valid: true}), wantInt64(7)},
		{"Int64FromNullable null", gcvctor.Int64FromNullable(spanner.NullInt64{}), wantNull(sppb.TypeCode_INT64)},
		{"Float64FromNullable value", gcvctor.Float64FromNullable(spanner.NullFloat64{Float64: 1.5, Valid: true}), wantFloat64(1.5)},
		{"Float64FromNullable null", gcvctor.Float64FromNullable(spanner.NullFloat64{}), wantNull(sppb.TypeCode_FLOAT64)},
		{"Float32FromNullable value", gcvctor.Float32FromNullable(spanner.NullFloat32{Float32: 2.5, Valid: true}), wantFloat32(2.5)},
		{"Float32FromNullable null", gcvctor.Float32FromNullable(spanner.NullFloat32{}), wantNull(sppb.TypeCode_FLOAT32)},
		{"StringFromNullable value", gcvctor.StringFromNullable(spanner.NullString{StringVal: "x", Valid: true}), wantString("x")},
		{"StringFromNullable null", gcvctor.StringFromNullable(spanner.NullString{}), wantNull(sppb.TypeCode_STRING)},
		{"DateFromNullable value", gcvctor.DateFromNullable(spanner.NullDate{Date: d, Valid: true}), wantDate(d)},
		{"DateFromNullable null", gcvctor.DateFromNullable(spanner.NullDate{}), wantNull(sppb.TypeCode_DATE)},
		{"TimestampFromNullable value", gcvctor.TimestampFromNullable(spanner.NullTime{Time: ts, Valid: true}), wantTimestamp(ts)},
		{"TimestampFromNullable null", gcvctor.TimestampFromNullable(spanner.NullTime{}), wantNull(sppb.TypeCode_TIMESTAMP)},
		{"UUIDFromNullable value", gcvctor.UUIDFromNullable(spanner.NullUUID{UUID: u, Valid: true}), wantUUID(u)},
		{"UUIDFromNullable null", gcvctor.UUIDFromNullable(spanner.NullUUID{}), wantNull(sppb.TypeCode_UUID)},
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
