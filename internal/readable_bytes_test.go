package internal

import (
	"strings"
	"testing"
)

func TestReadableBytesString_matchesEscapeRune(t *testing.T) {
	t.Parallel()

	var all [256]byte
	for i := range all {
		all[i] = byte(i)
	}
	for _, b := range [][]byte{
		nil,
		{},
		[]byte("abc"),
		[]byte("\x00\x01\x7f\xff"),
		all[:],
	} {
		got := ReadableBytesString(b)
		var want strings.Builder
		for _, c := range b {
			want.WriteString(EscapeRune(rune(c), false, -1))
		}
		if got != want.String() {
			t.Fatalf("ReadableBytesString(%q) = %q, want %q", b, got, want.String())
		}
	}
}

func TestReadableStringFromBase64Wire(t *testing.T) {
	t.Parallel()

	wire := "SGVsbG8="
	got, err := ReadableStringFromBase64Wire(wire)
	if err != nil {
		t.Fatal(err)
	}
	want := ReadableBytesString([]byte("Hello"))
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}

	_, err = ReadableStringFromBase64Wire("!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}

func TestReadableBytesString_readableASCIIFastPath(t *testing.T) {
	t.Parallel()

	const s = "payload-bytes"
	if got := ReadableBytesString([]byte(s)); got != s {
		t.Fatalf("got %q want %q", got, s)
	}
	got, err := ReadableStringFromBase64Wire("cGF5bG9hZC1ieXRlcw==") // "payload-bytes"
	if err != nil {
		t.Fatal(err)
	}
	if got != s {
		t.Fatalf("wire fast path: got %q want %q", got, s)
	}
}

func BenchmarkReadableStringFromBase64Wire(b *testing.B) {
	const wire = "SGVsbG8gV29ybGQh" // "Hello World!"
	b.ReportAllocs()
	for range b.N {
		if _, err := ReadableStringFromBase64Wire(wire); err != nil {
			b.Fatal(err)
		}
	}
}
