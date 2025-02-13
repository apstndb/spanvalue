package internal

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"strconv"

	"github.com/ngicks/go-iterator-helper/hiter/stringsiter"
	"github.com/ngicks/go-iterator-helper/x/exp/xiter"
	"github.com/samber/lo"

	"github.com/apstndb/spanvalue/internal/iterx"
)

func ByteToEscapeSequenceReadable(b byte) string {
	return lo.Ternary(strconv.IsPrint(rune(b)), string(b), fmt.Sprintf(`\x%02x`, b))
}

func Float64ToLiteral(v float64) string {
	switch {
	case math.IsNaN(v):
		return "CAST('nan' AS FLOAT64)"
	case math.IsInf(v, 1):
		return "CAST('inf' AS FLOAT64)"
	case math.IsInf(v, -1):
		return "CAST('-inf' AS FLOAT64)"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

func Float32ToLiteral(v float32) string {
	switch {
	case math.IsNaN(float64(v)):
		return "CAST('nan' AS FLOAT32)"
	case math.IsInf(float64(v), 1):
		return "CAST('inf' AS FLOAT32)"
	case math.IsInf(float64(v), -1):
		return "CAST('-inf' AS FLOAT32)"
	default:
		return fmt.Sprintf("CAST(%v AS FLOAT32)", strconv.FormatFloat(float64(v), 'g', -1, 32))
	}
}

func ToAny[T any](seq iter.Seq[T]) iter.Seq[any] {
	return xiter.Map(func(v T) any { return v }, seq)
}

func Pointers[T any, E ~[]T](e E) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for i := range len(e) {
			if !yield(&e[i]) {
				return
			}
		}
	}
}

func ToBytesLiteral(v []byte) string {
	return fmt.Sprintf(`b"%v"`, iterx.Joinf("", `\x%02x`, slices.Values(v)))
}

func ToReadableBytesLiteral(v []byte) string {
	return fmt.Sprintf(`b"%v"`, stringsiter.Collect(xiter.Map(ByteToEscapeSequenceReadable, slices.Values(v))))
}
