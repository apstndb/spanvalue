package gcvctor_test

import (
	"errors"
	"math/big"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPGNumericValueExact(t *testing.T) {
	t.Parallel()

	wantWire := func(wire string) spanner.GenericColumnValue {
		return spanner.GenericColumnValue{
			Type:  typector.PGNumeric(),
			Value: structpb.NewStringValue(wire),
		}
	}

	tests := []struct {
		name string
		in   *big.Rat
		want string
	}{
		{"integer", big.NewRat(42, 1), "42"},
		{"negative half", big.NewRat(-1, 2), "-0.5"},
		{"power of two denominator", big.NewRat(1, 8), "0.125"},
		{"mixed 2s and 5s", big.NewRat(1, 20), "0.05"},
		// 17 fractional digits: beyond the 9-digit NumericString scale that
		// PGNumericValue would silently round to.
		{"beyond GoogleSQL scale", new(big.Rat).SetFrac64(1, 1e17), "0.00000000000000001"},
		{"zero", big.NewRat(0, 1), "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := gcvctor.PGNumericValueExact(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(wantWire(tt.want), got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("non-terminating", func(t *testing.T) {
		t.Parallel()
		if _, err := gcvctor.PGNumericValueExact(big.NewRat(1, 3)); !errors.Is(err, gcvctor.ErrInexactNumeric) {
			t.Errorf("error = %v, want ErrInexactNumeric", err)
		}
		// 1/6 reduces to a denominator with factor 3.
		if _, err := gcvctor.PGNumericValueExact(big.NewRat(1, 6)); !errors.Is(err, gcvctor.ErrInexactNumeric) {
			t.Errorf("error = %v, want ErrInexactNumeric", err)
		}
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		if _, err := gcvctor.PGNumericValueExact(nil); !errors.Is(err, gcvctor.ErrNilNumeric) {
			t.Errorf("error = %v, want ErrNilNumeric", err)
		}
	})

	t.Run("contrast with PGNumericValue rounding", func(t *testing.T) {
		t.Parallel()
		v := new(big.Rat).SetFrac64(1, 1e17)
		rounded := gcvctor.PGNumericValue(v)
		if got := rounded.Value.GetStringValue(); got != "0.000000000" {
			t.Errorf("PGNumericValue wire = %q, want the 9-digit rounding this constructor exists to avoid", got)
		}
	})
}
