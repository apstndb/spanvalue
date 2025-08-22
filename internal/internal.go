package internal

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"strconv"
	"unicode"

	"github.com/ngicks/go-iterator-helper/hiter"
	"github.com/ngicks/go-iterator-helper/hiter/stringsiter"
)

// ByteToEscapeSequenceReadable formats a byte as a string without quote processing
func ByteToEscapeSequenceReadable(b byte) string {
	return EscapeRune(rune(b), false, -1)
}

func EscapeRune(r rune, isString bool, quote rune) string {
	switch {
	case r == quote || r == '\\':
		return `\` + string(r)
	case isString && r == '\n':
		return `\n`
	case isString && r == '\r':
		return `\r`
	case isString && r == '\t':
		return `\t`
	case isString && unicode.IsPrint(r):
		return string(r)
	// Even if !isString, printable 7-bit characters can be printed as-is.
	case 0x20 <= r && r <= 0x7E:
		return string(r)
	case r < 0x100:
		return fmt.Sprintf(`\x%02x`, r)
	case r > 0xFFFF:
		return fmt.Sprintf(`\U%08x`, r)
	default:
		return fmt.Sprintf(`\u%04x`, r)
	}
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
	return hiter.Map(func(v T) any { return v }, seq)
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

func suitableQuote(b []byte) rune {
	var hasDouble bool
	for _, r := range b {
		switch r {
		case '\'':
			return '"'
		case '"':
			hasDouble = true
		}
	}

	if hasDouble {
		return '\''
	}

	return '"'
}

func ToReadableBytesLiteral(v []byte) string {
	quote := suitableQuote(v)

	encoded := stringsiter.Collect(hiter.Map(func(b byte) string {
		return EscapeRune(rune(b), false, quote)
	}, slices.Values(v)))

	return fmt.Sprintf(`b%s%s%s`, string(quote), encoded, string(quote))
}

func ToStringLiteral(s string) string {
	quote := suitableQuote([]byte(s))

	encoded := stringsiter.Collect(hiter.Map(func(r rune) string {
		return EscapeRune(r, true, quote)
	}, slices.Values([]rune(s))))

	return fmt.Sprintf(`%s%s%s`, string(quote), encoded, string(quote))
}
