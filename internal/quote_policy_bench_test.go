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
		_ = quoteForPayload(policy, payload)
	}
}
