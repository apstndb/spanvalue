package internal

import (
	"encoding/base64"
	"sync"
)

// readableEscapedByte maps each byte to the same text as EscapeRune(b, false, -1).
var readableEscapedByte [256]string

func init() {
	for b := range readableEscapedByte {
		readableEscapedByte[b] = EscapeRune(rune(b), false, -1)
	}
}

func isReadableASCII(b []byte) bool {
	for _, c := range b {
		if c == '\\' || c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func appendReadableEscaped(dst []byte, b []byte) []byte {
	for _, c := range b {
		esc := readableEscapedByte[c]
		if len(esc) == 1 {
			dst = append(dst, esc[0])
		} else {
			dst = append(dst, esc...)
		}
	}
	return dst
}

// ReadableBytesString formats raw bytes for Simple-style output (no b" quotes).
func ReadableBytesString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if isReadableASCII(b) {
		return string(b)
	}
	out := make([]byte, 0, len(b)+len(b))
	out = appendReadableEscaped(out, b)
	return string(out)
}

var base64DecodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64)
		return &b
	},
}

func putDecodeBuf(bp *[]byte, buf []byte) {
	if cap(buf) > 4096 {
		*bp = nil
	} else {
		*bp = buf[:0]
	}
	base64DecodeBufPool.Put(bp)
}

// ReadableStringFromBase64Wire decodes a Spanner BYTES/PROTO base64 wire string and
// formats the payload with [ReadableBytesString].
func ReadableStringFromBase64Wire(wire string) (string, error) {
	if wire == "" {
		return "", nil
	}

	n := base64.StdEncoding.DecodedLen(len(wire))
	bp := base64DecodeBufPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < n {
		buf = make([]byte, n)
	} else {
		buf = buf[:n]
	}

	nw, err := base64.StdEncoding.Decode(buf, []byte(wire))
	if err != nil {
		putDecodeBuf(bp, buf)
		return "", err
	}
	decoded := buf[:nw]

	if isReadableASCII(decoded) {
		out := string(decoded)
		putDecodeBuf(bp, buf)
		return out, nil
	}

	out := make([]byte, 0, nw+nw)
	out = appendReadableEscaped(out, decoded)
	putDecodeBuf(bp, buf)
	return string(out), nil
}
