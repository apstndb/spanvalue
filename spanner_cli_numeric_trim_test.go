package spanvalue

import "testing"

func TestTrimSpannerCLINumericFraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, want string
	}{
		{"3.140", "3.14"},
		{"10.0", "10"},
		{"10.00", "10"},
		{"0.50", "0.5"},
		{"0.0", "0"},
		{"10", "10"},
		{"100", "100"},
		{"1000", "1000"},
		{"42", "42"},
		{"1.23", "1.23"},
		{"-10.50", "-10.5"},
	}
	for _, tc := range tests {
		if got := trimSpannerCLINumericFraction(tc.in); got != tc.want {
			t.Errorf("trimSpannerCLINumericFraction(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
