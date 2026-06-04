package internal

import (
	"strings"
	"testing"
)

// spannerMaxCellBytes is the Spanner STRING/BYTES cell-size limit (10 MiB).
const spannerMaxCellBytes = 10 * 1024 * 1024

func BenchmarkToStringLiteralPolicyMaxCell(b *testing.B) {
	payload := strings.Repeat("x", spannerMaxCellBytes)
	policy := QuotePolicy{}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		_ = ToStringLiteralPolicy(payload, policy)
	}
}

func BenchmarkQuoteForPayloadMaxCell(b *testing.B) {
	payload := strings.Repeat("x", spannerMaxCellBytes)
	policy := QuotePolicy{}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		_ = quoteForPayloadString(policy, payload)
	}
}

func BenchmarkQuoteForPayloadLegacyShapes(b *testing.B) {
	noQuotes := strings.Repeat("x", spannerMaxCellBytes)
	prefOnly := strings.Repeat("'", spannerMaxCellBytes)
	oppositeEarly := `"` + strings.Repeat("x", spannerMaxCellBytes-1)
	policy := QuotePolicy{}

	for _, tc := range []struct {
		name    string
		payload string
	}{
		{name: "no_quotes", payload: noQuotes},
		{name: "preferred_only", payload: prefOnly},
		{name: "opposite_early", payload: oppositeEarly},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("string", func(b *testing.B) {
				benchmarkQuoteForPayloadString(b, policy, tc.payload)
			})
			b.Run("bytes", func(b *testing.B) {
				benchmarkQuoteForPayloadBytes(b, policy, []byte(tc.payload))
			})
		})
	}
}

func benchmarkQuoteForPayloadString(b *testing.B, policy QuotePolicy, payload string) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		_ = quoteForPayloadString(policy, payload)
	}
}

func benchmarkQuoteForPayloadBytes(b *testing.B, policy QuotePolicy, payload []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		_ = quoteForPayloadBytes(policy, payload)
	}
}
