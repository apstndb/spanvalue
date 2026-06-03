package spanvalue

import "github.com/apstndb/spanvalue/internal"

// QuoteStrategy selects how the outer string-literal delimiter is chosen for the literal preset.
type QuoteStrategy uint8

const (
	// QuoteLegacy uses PreferredQuote when the payload needs no opposite-delimiter escape.
	// When the payload contains only the preferred quote character, the opposite delimiter is used.
	// PreferredDoubleQuote (the zero value) matches historical suitableQuote byte-for-byte.
	QuoteLegacy QuoteStrategy = iota
	// QuoteAlways uses PreferredQuote for every string and bytes literal.
	QuoteAlways
	// QuoteMinEscape picks the delimiter whose quote character occurs less often in the payload.
	// On a tie, PreferredQuote wins. Only quote-character counts matter; other escapes are delimiter-independent.
	QuoteMinEscape
)

// PreferredQuote is the default delimiter for [QuoteLegacy] (with opposite-delimiter escape
// when the payload contains only that quote character), the fixed delimiter for [QuoteAlways],
// and the tie-breaker for [QuoteMinEscape].
type PreferredQuote uint8

const (
	// PreferredDoubleQuote uses double quotes as the outer delimiter.
	PreferredDoubleQuote PreferredQuote = iota
	// PreferredSingleQuote uses single quotes as the outer delimiter.
	PreferredSingleQuote
)

// LiteralQuoteConfig configures string and bytes literal quoting for the literal preset.
// The zero value is legacy adaptive quoting. Invalid enum values are normalized per axis.
type LiteralQuoteConfig struct {
	Strategy       QuoteStrategy
	PreferredQuote PreferredQuote
}

// LiteralFormatOptions holds settings that apply only to the literal preset ([LiteralFormatConfig]
// and clones). Other presets ignore this field. A future major release may move literal-specific
// configuration off [FormatConfig] entirely; callers should set options via literal constructors
// or [WithLiteralQuote] rather than assuming a stable nested layout.
type LiteralFormatOptions struct {
	// Quote selects the outer delimiter policy for string and bytes SQL-style literals.
	Quote LiteralQuoteConfig
}

// LiteralOption configures a literal preset returned by [LiteralFormatConfigWithOptions].
type LiteralOption interface {
	applyLiteralOption(*FormatConfig)
}

type literalQuoteOption struct {
	cfg LiteralQuoteConfig
}

// WithLiteralQuote sets quote policy on a literal preset built with [LiteralFormatConfigWithOptions].
func WithLiteralQuote(cfg LiteralQuoteConfig) LiteralOption {
	return literalQuoteOption{cfg: cfg}
}

func (o literalQuoteOption) applyLiteralOption(fc *FormatConfig) {
	fc.Literal.Quote = normalizeLiteralQuote(o.cfg)
}

// LiteralFormatConfigWithOptions returns a copy of [LiteralFormatConfig] with the given options applied.
func LiteralFormatConfigWithOptions(opts ...LiteralOption) *FormatConfig {
	fc := LiteralFormatConfig()
	for _, opt := range opts {
		if opt != nil {
			opt.applyLiteralOption(fc)
		}
	}
	return fc
}

// LiteralFormatConfigWithQuote returns a copy of [LiteralFormatConfig] with the given quote settings.
func LiteralFormatConfigWithQuote(cfg LiteralQuoteConfig) *FormatConfig {
	return LiteralFormatConfigWithOptions(WithLiteralQuote(cfg))
}

// LiteralFormatConfigWithSingleQuotedLiterals returns a literal preset that always single-quotes
// string and bytes literals (SQL INSERT style).
func LiteralFormatConfigWithSingleQuotedLiterals() *FormatConfig {
	return LiteralFormatConfigWithQuote(LiteralQuoteConfig{
		Strategy:       QuoteAlways,
		PreferredQuote: PreferredSingleQuote,
	})
}

func (s QuoteStrategy) String() string {
	switch s {
	case QuoteLegacy:
		return "QuoteLegacy"
	case QuoteAlways:
		return "QuoteAlways"
	case QuoteMinEscape:
		return "QuoteMinEscape"
	default:
		return "QuoteStrategy(" + itoaUint8(uint8(s)) + ")"
	}
}

func (p PreferredQuote) String() string {
	switch p {
	case PreferredDoubleQuote:
		return "PreferredDoubleQuote"
	case PreferredSingleQuote:
		return "PreferredSingleQuote"
	default:
		return "PreferredQuote(" + itoaUint8(uint8(p)) + ")"
	}
}

func itoaUint8(v uint8) string {
	const digits = "0123456789"
	if v < 10 {
		return string(digits[v])
	}
	return string([]byte{digits[v/10], digits[v%10]})
}

func normalizeLiteralQuote(cfg LiteralQuoteConfig) LiteralQuoteConfig {
	switch cfg.Strategy {
	case QuoteLegacy, QuoteAlways, QuoteMinEscape:
	default:
		cfg.Strategy = QuoteLegacy
	}
	switch cfg.PreferredQuote {
	case PreferredDoubleQuote, PreferredSingleQuote:
	default:
		cfg.PreferredQuote = PreferredDoubleQuote
	}
	return cfg
}

func toInternalQuotePolicy(cfg LiteralQuoteConfig) internal.QuotePolicy {
	cfg = normalizeLiteralQuote(cfg)
	return internal.QuotePolicy{
		Strategy:  internal.QuoteStrategy(cfg.Strategy),
		Preferred: internal.PreferredQuote(cfg.PreferredQuote),
	}
}

func literalQuoteForFormatter(formatter any) LiteralQuoteConfig {
	fc, ok := formatter.(*FormatConfig)
	if !ok {
		return LiteralQuoteConfig{}
	}
	return fc.Literal.Quote
}
