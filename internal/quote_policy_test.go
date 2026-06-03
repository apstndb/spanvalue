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
			got := minEscapeQuote([]byte(tt.payload), tt.preferred)
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
		got := legacyQuote([]byte(payload), PreferredQuoteDouble)
		want := suitableQuote([]byte(payload))
		if got != want {
			t.Fatalf("legacyQuote(%q, double) = %q, suitableQuote = %q", payload, got, want)
		}
	}
}

func TestQuotePolicyMinEscapeDiffersFromLegacyOnBothQuotes(t *testing.T) {
	t.Parallel()

	payload := []byte(`"it's"`)
	legacy := QuotePolicy{}.quoteForPayload(payload)
	minEscape := QuotePolicy{Strategy: QuoteStrategyMinEscape, Preferred: PreferredQuoteDouble}.quoteForPayload(payload)

	if legacy != '"' {
		t.Fatalf("legacy = %q, want double (first single wins)", legacy)
	}
	if minEscape != '\'' {
		t.Fatalf("minEscape = %q, want single (more doubles than singles)", minEscape)
	}
}
