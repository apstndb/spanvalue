package spanvalue

import "strings"

// trimSpannerCLINumericFraction trims trailing zeros in the fractional part of a
// NUMERIC wire string for Spanner CLI output (e.g. "3.140" → "3.14", "10.0" → "10").
// Integer values without a decimal point are unchanged ("10" stays "10").
func trimSpannerCLINumericFraction(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}
