package spanvalue

import (
	"strconv"

	"github.com/apstndb/spanvalue/internal"
)

// QuoteStrategy selects how the outer string-literal delimiter is chosen for the literal preset.
type QuoteStrategy uint8

const (
	// QuoteLegacy uses PreferredQuote when the payload needs no opposite-delimiter escape.
	// When the payload contains only the preferred quote character, the opposite delimiter is used.
	// PreferredDoubleQuote (the zero value) matches historical suitableQuote byte-for-byte.
	// When both quote characters appear, Legacy uses presence rules (any opposite quote keeps
	// the preferred delimiter); [QuoteMinEscape] instead compares quote-character counts.
	QuoteLegacy QuoteStrategy = iota
	// QuoteAlways uses PreferredQuote for every string and bytes literal.
	QuoteAlways
	// QuoteMinEscape picks the delimiter whose quote character occurs less often in the payload.
	// On a tie, PreferredQuote wins. Only quote-character counts matter; other escapes are delimiter-independent.
	// With PreferredSingleQuote it often matches [QuoteLegacy], but when both delimiters appear
	// MinEscape compares counts (e.g. a''b"c stays single-quoted under Legacy+Single but uses
	// double under MinEscape+Single because one double escape beats two single escapes).
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

// LiteralFormatOptions holds settings that apply only to the literal preset.
// It is a constructor input ([LiteralFormatConfigWithOptions],
// [LiteralValuePlugin]): the options are captured into the literal preset's
// quote-sensitive plugins at construction time, not stored on [FormatConfig].
type LiteralFormatOptions struct {
	// Quote selects the outer delimiter policy for string and bytes SQL-style literals.
	// The zero value is legacy adaptive quoting (QuoteLegacy + PreferredDoubleQuote);
	// invalid enum values are normalized when options are applied. Escaping uses
	// GoogleSQL backslash rules; not PostgreSQL (#126).
	Quote LiteralQuoteConfig
}

// LiteralOption configures a literal preset returned by [LiteralFormatConfigWithOptions].
type LiteralOption interface {
	applyLiteralOption(*LiteralFormatOptions)
}

type literalQuoteOption struct {
	cfg LiteralQuoteConfig
}

// WithLiteralQuote sets quote policy on a literal preset built with [LiteralFormatConfigWithOptions].
func WithLiteralQuote(cfg LiteralQuoteConfig) LiteralOption {
	return literalQuoteOption{cfg: cfg}
}

func (o literalQuoteOption) applyLiteralOption(opts *LiteralFormatOptions) {
	opts.Quote = normalizeLiteralQuote(o.cfg)
}

// LiteralFormatConfigWithOptions returns a [LiteralFormatConfig] preset with the given
// options captured into its quote-sensitive plugins (string/bytes scalar literals and
// PROTO casts).
func LiteralFormatConfigWithOptions(opts ...LiteralOption) *FormatConfig {
	var o LiteralFormatOptions
	for _, opt := range opts {
		if opt != nil {
			opt.applyLiteralOption(&o)
		}
	}
	return literalFormatConfigFromOptions(o)
}

// LiteralFormatConfigWithQuote returns a [LiteralFormatConfig] preset with the given quote settings.
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
		return "QuoteStrategy(" + strconv.Itoa(int(s)) + ")"
	}
}

func (p PreferredQuote) String() string {
	switch p {
	case PreferredDoubleQuote:
		return "PreferredDoubleQuote"
	case PreferredSingleQuote:
		return "PreferredSingleQuote"
	default:
		return "PreferredQuote(" + strconv.Itoa(int(p)) + ")"
	}
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
	var p internal.QuotePolicy
	switch cfg.Strategy {
	case QuoteAlways:
		p.Strategy = internal.QuoteStrategyAlways
	case QuoteMinEscape:
		p.Strategy = internal.QuoteStrategyMinEscape
	default:
		p.Strategy = internal.QuoteStrategyLegacy
	}
	switch cfg.PreferredQuote {
	case PreferredSingleQuote:
		p.Preferred = internal.PreferredQuoteSingle
	default:
		p.Preferred = internal.PreferredQuoteDouble
	}
	return p
}
