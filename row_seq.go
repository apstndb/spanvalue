package spanvalue

import (
	"errors"
	"iter"

	"cloud.google.com/go/spanner"
)

// ErrNilRowSeq reports that a row sequence is nil.
var ErrNilRowSeq = errors.New("nil row sequence")

// FormatRowSeq lazily formats each row from rows with fc. It is the sequence
// counterpart of [*FormatConfig.FormatRow].
//
// A non-nil source error is yielded unchanged and terminates the sequence; the
// row paired with that error is ignored. A nil row or formatting error is also
// yielded and terminates the sequence. If the consumer stops early, FormatRowSeq
// stops immediately without draining rows. FormatRowSeq emits values only; it
// does not synthesize a header row.
//
// A nil receiver yields [ErrNilFormatConfig]. A nil rows sequence yields
// [ErrNilRowSeq].
func (fc *FormatConfig) FormatRowSeq(rows iter.Seq2[*spanner.Row, error]) iter.Seq2[[]string, error] {
	return func(yield func([]string, error) bool) {
		if fc == nil {
			yield(nil, ErrNilFormatConfig)
			return
		}
		if rows == nil {
			yield(nil, ErrNilRowSeq)
			return
		}

		for row, err := range rows {
			if err != nil {
				yield(nil, err)
				return
			}
			values, err := fc.FormatRow(row)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(values, nil) {
				return
			}
		}
	}
}
