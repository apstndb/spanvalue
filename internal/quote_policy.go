package internal

import (
	"bytes"
	"strings"
)

type bytesSeq interface {
	~string | ~[]byte
}

// QuoteStrategy selects how the outer string-literal delimiter is chosen.
type QuoteStrategy uint8

const (
	QuoteStrategyLegacy QuoteStrategy = iota
	QuoteStrategyAlways
	QuoteStrategyMinEscape
)

// PreferredQuote is the delimiter used by [QuoteStrategyAlways] and as tie-breaker for [QuoteStrategyMinEscape].
type PreferredQuote uint8

const (
	PreferredQuoteDouble PreferredQuote = iota
	PreferredQuoteSingle
)

// QuotePolicy configures literal string delimiters. The zero value is legacy adaptive quoting.
type QuotePolicy struct {
	Strategy  QuoteStrategy
	Preferred PreferredQuote
}

func quoteForPayloadString(p QuotePolicy, payload string) rune {
	switch p.Strategy {
	case QuoteStrategyAlways:
		if p.Preferred == PreferredQuoteSingle {
			return '\''
		}
		return '"'
	case QuoteStrategyMinEscape:
		return minEscapeQuote(payload, p.Preferred)
	default:
		return legacyQuoteString(payload, p.Preferred)
	}
}

func quoteForPayloadBytes(p QuotePolicy, payload []byte) rune {
	switch p.Strategy {
	case QuoteStrategyAlways:
		if p.Preferred == PreferredQuoteSingle {
			return '\''
		}
		return '"'
	case QuoteStrategyMinEscape:
		return minEscapeQuote(payload, p.Preferred)
	default:
		return legacyQuoteBytes(payload, p.Preferred)
	}
}

// quoteForPayload dispatches to typed helpers. Used by tests; hot paths call
// quoteForPayloadString or quoteForPayloadBytes directly to avoid interface boxing.
func quoteForPayload[T bytesSeq](p QuotePolicy, payload T) rune {
	switch val := any(payload).(type) {
	case string:
		return quoteForPayloadString(p, val)
	case []byte:
		return quoteForPayloadBytes(p, val)
	default:
		panic("unreachable")
	}
}

// legacyQuoteString generalizes historical suitableQuote for string payloads.
func legacyQuoteString(payload string, preferred PreferredQuote) rune {
	pref, other := byte('"'), byte('\'')
	if preferred == PreferredQuoteSingle {
		pref, other = '\'', '"'
	}
	if strings.IndexByte(payload, other) >= 0 {
		return rune(pref)
	}
	if strings.IndexByte(payload, pref) >= 0 {
		return rune(other)
	}
	return rune(pref)
}

// legacyQuoteBytes generalizes historical suitableQuote for byte payloads.
func legacyQuoteBytes(payload []byte, preferred PreferredQuote) rune {
	pref, other := byte('"'), byte('\'')
	if preferred == PreferredQuoteSingle {
		pref, other = '\'', '"'
	}
	if bytes.IndexByte(payload, other) >= 0 {
		return rune(pref)
	}
	if bytes.IndexByte(payload, pref) >= 0 {
		return rune(other)
	}
	return rune(pref)
}

// legacyQuote dispatches to typed legacy quote selection. Used by tests.
func legacyQuote[T bytesSeq](payload T, preferred PreferredQuote) rune {
	switch val := any(payload).(type) {
	case string:
		return legacyQuoteString(val, preferred)
	case []byte:
		return legacyQuoteBytes(val, preferred)
	default:
		panic("unreachable")
	}
}

// minEscapeQuote picks the delimiter that occurs less often in the payload.
// On a tie (including payloads with neither quote character), PreferredQuote wins.
func minEscapeQuote[T bytesSeq](payload T, preferred PreferredQuote) rune {
	var singleCount, doubleCount int
	for i := 0; i < len(payload); i++ {
		b := payload[i]
		switch b {
		case '\'':
			singleCount++
		case '"':
			doubleCount++
		}
	}
	switch {
	case singleCount < doubleCount:
		return '\''
	case doubleCount < singleCount:
		return '"'
	default:
		if preferred == PreferredQuoteSingle {
			return '\''
		}
		return '"'
	}
}
