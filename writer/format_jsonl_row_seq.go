package writer

import (
	"iter"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
)

// FormatJSONLRowSeq lazily formats each row from rows as a JSON object string.
// It is the sequence counterpart of [FormatJSONLRow]; each successful value
// has no trailing newline. A nil fc uses [spanvalue.JSONFormatConfig].
//
// A non-nil source error is yielded unchanged and terminates the sequence; the
// row paired with that error is ignored. A nil row or formatting error is also
// yielded and terminates the sequence. If the consumer stops early,
// FormatJSONLRowSeq stops immediately without draining rows.
//
// A nil rows sequence yields [ErrNilRowSeq].
func FormatJSONLRowSeq(fc *spanvalue.FormatConfig, rows iter.Seq2[*spanner.Row, error], namer spanvalue.UnnamedFieldNamer) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if rows == nil {
			yield("", ErrNilRowSeq)
			return
		}

		for row, err := range rows {
			if err != nil {
				yield("", err)
				return
			}
			formatted, err := FormatJSONLRow(fc, row, namer)
			if err != nil {
				yield("", err)
				return
			}
			if !yield(formatted, nil) {
				return
			}
		}
	}
}
