package internal

type bytesSeq interface {
	string | []byte
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

func quoteForPayload[T bytesSeq](p QuotePolicy, payload T) rune {
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
func legacyQuote[T bytesSeq](payload T, preferred PreferredQuote) rune {
	pref, other := byte('"'), byte('\'')
	if preferred == PreferredQuoteSingle {
		pref, other = '\'', '"'
	}
	var hasPref bool
	for i := 0; i < len(payload); i++ {
		b := payload[i]
		switch b {
		case other:
			return rune(pref)
		case pref:
			hasPref = true
		}
	}
	if hasPref {
		return rune(other)
	}
	return rune(pref)
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
