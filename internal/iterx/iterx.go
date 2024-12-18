package iterx

import (
	"fmt"
	"iter"
	"strings"

	"github.com/ngicks/go-iterator-helper/x/exp/xiter"
)

func Joinf[T any](sep string, format string, seq iter.Seq[T]) string {
	return Join(sep, xiter.Map(func(v T) string {
		return fmt.Sprintf(format, v)
	}, seq))
}

func Join(sep string, seq iter.Seq[string]) string {
	var sb strings.Builder
	first := true
	for v := range seq {
		if first {
			first = false
		} else {
			sb.WriteString(sep)
		}
		sb.WriteString(v)
	}
	return sb.String()
}
