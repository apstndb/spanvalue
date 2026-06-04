package internal

import "testing"

func TestMinEscapeQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		payload   string
		preferred PreferredQuote
		want      rune
	}{
		{name: "more singles prefers double", payload: "a'b", preferred: PreferredQuoteSingle, want: '"'},
		{name: "more doubles prefers single", payload: `a"b"c`, preferred: PreferredQuoteDouble, want: '\''},
		{name: "tie no quotes prefers double", payload: "plain", preferred: PreferredQuoteDouble, want: '"'},
		{name: "tie no quotes prefers single", payload: "plain", preferred: PreferredQuoteSingle, want: '\''},
		{name: "equal counts tie prefers double", payload: `'x"y`, preferred: PreferredQuoteDouble, want: '"'},
		{name: "equal counts tie prefers single", payload: `'x"y`, preferred: PreferredQuoteSingle, want: '\''},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := minEscapeQuote(tt.payload, tt.preferred)
			if got != tt.want {
				t.Fatalf("minEscapeQuote(%q, %v) = %q, want %q", tt.payload, tt.preferred, got, tt.want)
			}
		})
	}
}

func TestLegacyQuoteMatchesSuitableQuoteWithDoublePreferred(t *testing.T) {
	t.Parallel()

	payloads := []string{
		"plain",
		"it's",
		`say "hi"`,
		`"it's"`,
		"no quotes here",
		`'alone'`,
		`""only doubles""`,
	}
	for _, payload := range payloads {
		got := legacyQuote(payload, PreferredQuoteDouble)
		want := suitableQuote([]byte(payload))
		if got != want {
			t.Fatalf("legacyQuote(%q, double) = %q, suitableQuote = %q", payload, got, want)
		}
	}
}

func TestQuotePolicyMinEscapeDiffersFromLegacyOnBothQuotes(t *testing.T) {
	t.Parallel()

	payload := `"it's"`
	legacy := quoteForPayload(QuotePolicy{}, payload)
	minEscape := quoteForPayload(QuotePolicy{Strategy: QuoteStrategyMinEscape, Preferred: PreferredQuoteDouble}, payload)

	if legacy != '"' {
		t.Fatalf("legacy = %q, want double (first single wins)", legacy)
	}
	if minEscape != '\'' {
		t.Fatalf("minEscape = %q, want single (more doubles than singles)", minEscape)
	}
}

func TestQuoteForPayloadStringMatchesBytes(t *testing.T) {
	t.Parallel()

	payloads := []string{"plain", "it's", `say "hi"`, `"it's"`}
	policies := []QuotePolicy{
		{},
		{Strategy: QuoteStrategyAlways, Preferred: PreferredQuoteSingle},
		{Strategy: QuoteStrategyMinEscape, Preferred: PreferredQuoteDouble},
	}
	for _, policy := range policies {
		for _, payload := range payloads {
			gotStr := quoteForPayloadString(policy, payload)
			gotBytes := quoteForPayloadBytes(policy, []byte(payload))
			if gotStr != gotBytes {
				t.Fatalf("policy=%+v payload=%q: string=%q bytes=%q", policy, payload, gotStr, gotBytes)
			}
		}
	}
}

func TestLegacyQuoteStringMatchesBytes(t *testing.T) {
	t.Parallel()

	payloads := []string{"plain", "it's", `say "hi"`, `"it's"`}
	for _, preferred := range []PreferredQuote{PreferredQuoteDouble, PreferredQuoteSingle} {
		for _, payload := range payloads {
			gotStr := legacyQuoteString(payload, preferred)
			gotBytes := legacyQuoteBytes([]byte(payload), preferred)
			if gotStr != gotBytes {
				t.Fatalf("preferred=%v payload=%q: string=%q bytes=%q", preferred, payload, gotStr, gotBytes)
			}
		}
	}
}
