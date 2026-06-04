package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"cloud.google.com/go/spanner"
	"github.com/samber/lo/it"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrMismatchedJSONObjectFields = errors.New("mismatched JSON object key/value count")

// IsNullGenericColumnValue reports whether gcv represents SQL NULL.
// A nil gcv.Value is treated as NULL.
func IsNullGenericColumnValue(gcv spanner.GenericColumnValue) bool {
	if gcv.Value == nil {
		return true
	}
	_, ok := gcv.Value.GetKind().(*structpb.Value_NullValue)
	return ok
}

// ResolveColumnNames returns a copy of columnNames with every empty string
// replaced by a name produced by namer. Already-named columns are preserved.
// If namer is nil the input slice is returned unchanged without copying.
func ResolveColumnNames(columnNames []string, namer func(int) string) ([]string, error) {
	if namer == nil {
		return columnNames, nil
	}
	return ResolveColumnNamesInPlace(slices.Clone(columnNames), namer)
}

// ResolveColumnNamesInPlace resolves unnamed columns in names directly.
// If namer is nil the input slice is returned unchanged.
func ResolveColumnNamesInPlace(names []string, namer func(int) string) ([]string, error) {
	if namer == nil {
		return names, nil
	}

	usedNames := make(map[string]bool, len(names))
	for _, name := range names {
		if name != "" {
			usedNames[name] = true
		}
	}

	autoIdx := 0
	var attempted map[string]bool
	for i, name := range names {
		if name != "" {
			continue
		}
		if attempted == nil {
			attempted = make(map[string]bool)
		} else {
			clear(attempted)
		}
		for {
			name = namer(autoIdx)
			autoIdx++
			if name == "" {
				return nil, fmt.Errorf("unnamed field namer returned empty string (field index %d, generated index %d)", i, autoIdx-1)
			}
			if !usedNames[name] {
				break
			}
			if attempted[name] {
				return nil, fmt.Errorf("unnamed field namer returned repeated colliding name %q (field index %d, generated index %d)", name, i, autoIdx-1)
			}
			attempted[name] = true
		}
		names[i] = name
		usedNames[name] = true
	}

	return names, nil
}

// AssembleResolvedJSONObject combines already-resolved JSON object keys and
// pre-formatted JSON value strings into a single JSON object string.
func AssembleResolvedJSONObject(columnNames []string, values []string) (string, error) {
	marshaledKeys, err := MarshalJSONObjectKeys(columnNames)
	if err != nil {
		return "", err
	}
	return AssembleJSONObjectWithMarshaledKeys(marshaledKeys, values)
}

// MarshalJSONObjectKeys marshals JSON object keys once for reuse across rows.
func MarshalJSONObjectKeys(columnNames []string) ([][]byte, error) {
	keys := make([][]byte, len(columnNames))
	for i, name := range columnNames {
		// While json.Marshal on a Go string is technically infallible, we check the error for robustness.
		// Note: strconv.Quote is not suitable here because it produces Go string
		// literal escapes (e.g., \a, \v) that are not valid JSON escape sequences.
		key, err := json.Marshal(name)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

// AssembleJSONObjectWithMarshaledKeys combines pre-marshaled JSON object keys
// and pre-formatted JSON value strings into a single JSON object string.
func AssembleJSONObjectWithMarshaledKeys(keys [][]byte, values []string) (string, error) {
	if len(keys) != len(values) {
		return "", fmt.Errorf("%w: %d keys, %d values", ErrMismatchedJSONObjectFields, len(keys), len(values))
	}

	var b strings.Builder
	// Grow uses a cheap lower bound only. Key/value sizes are content-dependent.
	b.Grow(len(values))
	b.WriteByte('{')
	for i, val := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(keys[i])
		b.WriteByte(':')
		b.WriteString(val)
	}
	b.WriteByte('}')
	return b.String(), nil
}

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
	return Float64ToLiteralPolicy(v, QuotePolicy{})
}

func Float64ToLiteralPolicy(v float64, policy QuotePolicy) string {
	switch {
	case math.IsNaN(v):
		return sqlCastQuotedString("nan", "FLOAT64", nonFiniteCastDelimiter(policy))
	case math.IsInf(v, 1):
		return sqlCastQuotedString("inf", "FLOAT64", nonFiniteCastDelimiter(policy))
	case math.IsInf(v, -1):
		return sqlCastQuotedString("-inf", "FLOAT64", nonFiniteCastDelimiter(policy))
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

func Float32ToLiteral(v float32) string {
	return Float32ToLiteralPolicy(v, QuotePolicy{})
}

func Float32ToLiteralPolicy(v float32, policy QuotePolicy) string {
	switch {
	case math.IsNaN(float64(v)):
		return sqlCastQuotedString("nan", "FLOAT32", nonFiniteCastDelimiter(policy))
	case math.IsInf(float64(v), 1):
		return sqlCastQuotedString("inf", "FLOAT32", nonFiniteCastDelimiter(policy))
	case math.IsInf(float64(v), -1):
		return sqlCastQuotedString("-inf", "FLOAT32", nonFiniteCastDelimiter(policy))
	default:
		return fmt.Sprintf("CAST(%v AS FLOAT32)", strconv.FormatFloat(float64(v), 'g', -1, 32))
	}
}

// nonFiniteCastDelimiter selects the outer delimiter for NaN/±Inf CAST payloads.
// v0.5 compat: QuoteStrategyLegacy keeps historical single quotes; Always and
// MinEscape use PreferredQuote. Planned v0.6: align with quoteForPayload.
func nonFiniteCastDelimiter(policy QuotePolicy) rune {
	switch policy.Strategy {
	case QuoteStrategyAlways, QuoteStrategyMinEscape:
		if policy.Preferred == PreferredQuoteDouble {
			return '"'
		}
		return '\''
	default:
		return '\''
	}
}

func sqlCastQuotedString(payload, castType string, quote rune) string {
	var b strings.Builder
	b.Grow(len(payload) + len(castType) + 12)
	b.WriteString("CAST(")
	b.WriteRune(quote)
	b.WriteString(payload)
	b.WriteRune(quote)
	b.WriteString(" AS ")
	b.WriteString(castType)
	b.WriteByte(')')
	return b.String()
}

func ToAny[T any](seq iter.Seq[T]) iter.Seq[any] {
	return it.ToAnySeq(seq)
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
	return legacyQuote(b, PreferredQuoteDouble)
}

func ToReadableBytesLiteral(v []byte) string {
	return ToReadableBytesLiteralPolicy(v, QuotePolicy{})
}

func ToReadableBytesLiteralPolicy(v []byte, policy QuotePolicy) string {
	quote := quoteForPayloadBytes(policy, v)

	var encoded strings.Builder
	// Grow uses a cheap lower bound only. Escape expansion is content-dependent,
	// so larger multipliers are speculative unless profiling shows a benefit.
	encoded.Grow(len(v) + 3)
	encoded.WriteByte('b')
	encoded.WriteRune(quote)
	for _, b := range v {
		encoded.WriteString(EscapeRune(rune(b), false, quote))
	}
	encoded.WriteRune(quote)

	return encoded.String()
}

func ToStringLiteral(s string) string {
	return ToStringLiteralPolicy(s, QuotePolicy{})
}

func ToStringLiteralPolicy(s string, policy QuotePolicy) string {
	quote := quoteForPayloadString(policy, s)

	var encoded strings.Builder
	// Grow uses a cheap lower bound only. Escape expansion is content-dependent,
	// so larger multipliers are speculative unless profiling shows a benefit.
	encoded.Grow(len(s) + 2)
	encoded.WriteRune(quote)
	for _, r := range s {
		encoded.WriteString(EscapeRune(r, true, quote))
	}
	encoded.WriteRune(quote)

	return encoded.String()
}
