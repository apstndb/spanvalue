package spanvalue

import (
	"errors"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJSONSpecialScalarMalformedWire(t *testing.T) {
	t.Parallel()
	for _, code := range []sppb.TypeCode{sppb.TypeCode_BOOL, sppb.TypeCode_INT64, sppb.TypeCode_ENUM, sppb.TypeCode_JSON} {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			_, err := JSONFormatConfig().FormatToplevelColumn(spanner.GenericColumnValue{Type: &sppb.Type{Code: code}, Value: structpb.NewListValue(&structpb.ListValue{})})
			if !errors.Is(err, ErrMalformedWire) || errors.Is(err, ErrUnknownType) {
				t.Errorf("malformed kind classification: %v", err)
			}
		})
	}
	for _, code := range []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_ENUM} {
		for _, tt := range []struct {
			wire  string
			cause error
		}{{"12.5", strconv.ErrSyntax}, {"9223372036854775808", strconv.ErrRange}} {
			t.Run(code.String()+"/"+tt.wire, func(t *testing.T) {
				t.Parallel()
				_, err := JSONFormatConfig().FormatToplevelColumn(spanner.GenericColumnValue{Type: &sppb.Type{Code: code}, Value: structpb.NewStringValue(tt.wire)})
				var numErr *strconv.NumError
				if !errors.Is(err, ErrMalformedWire) || !errors.Is(err, tt.cause) || !errors.As(err, &numErr) {
					t.Fatalf("error lost classification or parse cause: %v", err)
				}
				if numErr.Num != tt.wire {
					t.Errorf("Num=%q, want %q", numErr.Num, tt.wire)
				}
			})
		}
	}
}

func TestJSONSpecialScalarPreservesWire(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		code sppb.TypeCode
		wire string
	}{{sppb.TypeCode_INT64, " 42 "}, {sppb.TypeCode_ENUM, "\n-1\t"}, {sppb.TypeCode_JSON, "{\n \"b\":1, \"a\":2\n}"}} {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()
			got, err := JSONFormatConfig().FormatToplevelColumn(spanner.GenericColumnValue{Type: &sppb.Type{Code: tt.code}, Value: structpb.NewStringValue(tt.wire)})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wire, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}
