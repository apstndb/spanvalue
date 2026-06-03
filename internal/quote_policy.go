package internal

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

func (p QuotePolicy) quoteForPayload(payload []byte) rune {
	switch p.Strategy {
	case QuoteStrategyAlways:
		if p.Preferred == PreferredQuoteSingle {
			return '\''
		}
		return '"'
	case QuoteStrategyMinEscape:
		return minEscapeQuote(payload, p.Preferred)
	default:
		return legacyQuote(payload, p.Preferred)
	}
}

// legacyQuote generalizes historical suitableQuote: when the payload contains only the
// preferred delimiter character, use the opposite delimiter; otherwise use preferred.
// With PreferredQuoteDouble this matches suitableQuote byte-for-byte.
func legacyQuote(payload []byte, preferred PreferredQuote) rune {
	pref, other := byte('"'), byte('\'')
	if preferred == PreferredQuoteSingle {
		pref, other = '\'', '"'
	}
	var hasPref, hasOther bool
	for _, b := range payload {
		switch b {
		case pref:
			hasPref = true
		case other:
			hasOther = true
		}
	}
	if hasPref && !hasOther {
		return rune(other)
	}
	return rune(pref)
}

// minEscapeQuote picks the delimiter that occurs less often in the payload.
// On a tie (including payloads with neither quote character), PreferredQuote wins.
func minEscapeQuote(payload []byte, preferred PreferredQuote) rune {
	var singleCount, doubleCount int
	for _, r := range payload {
		switch r {
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

// quoteForSpecialFloatCast selects the delimiter for NaN/±Inf CAST payloads.
// Legacy stays single-quoted. QuoteAlways and QuoteMinEscape use PreferredQuote
// (MinEscape payloads have no quote characters, so preferred is the tie-breaker).
func (p QuotePolicy) quoteForSpecialFloatCast() rune {
	switch p.Strategy {
	case QuoteStrategyAlways, QuoteStrategyMinEscape:
		if p.Preferred == PreferredQuoteDouble {
			return '"'
		}
		return '\''
	default:
		return '\''
	}
}
