package writer

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spanvalue/gcvctor"
)

// errInjected is the sentinel returned by failNthWrite.
var errInjected = errors.New("injected write failure")

// failNthWrite is an io.Writer that fails the n-th Write call (1-based) with
// errInjected, writing nothing for that call, and passes every other call
// through to buf. It models a sink that fails once and then recovers, so tests
// can prove that writers stay failed (sticky error) instead of resuming output.
type failNthWrite struct {
	buf   bytes.Buffer
	n     int
	calls int
}

func (w *failNthWrite) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.n {
		return 0, errInjected
	}
	return w.buf.Write(p)
}

// valuesFlushWriter is the common surface of the three writers used by the
// sticky-error table test.
type valuesFlushWriter interface {
	WriteValues(columnNames []string, values []spanner.GenericColumnValue) error
	Flush() error
}

func TestWritersStickyErrorAfterWriteFailure(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id"}
	row := []spanner.GenericColumnValue{gcvctor.Int64Value(1)}

	tests := []struct {
		name  string
		build func(out io.Writer) (valuesFlushWriter, error)
	}{
		{
			name: "DelimitedWriter",
			build: func(out io.Writer) (valuesFlushWriter, error) {
				// WithFlushEachRow surfaces the I/O error on the failing row;
				// WithHeader(false) makes the data row the first output write.
				return NewDelimitedWriter(out, Comma, WithHeader(false), WithFlushEachRow())
			},
		},
		{
			name: "JSONLWriter",
			build: func(out io.Writer) (valuesFlushWriter, error) {
				return NewJSONLWriter(out)
			},
		},
		{
			name: "SQLInsertWriter",
			build: func(out io.Writer) (valuesFlushWriter, error) {
				return NewSQLInsertWriter(out, "users")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := &failNthWrite{n: 1}
			w, err := tt.build(out)
			if err != nil {
				t.Fatalf("constructor error = %v", err)
			}

			if err := w.WriteValues(columnNames, row); !errors.Is(err, errInjected) {
				t.Fatalf("first WriteValues() error = %v, want errInjected", err)
			}
			lenAfterFailure := out.buf.Len()

			// The sink has recovered, but the writer must stay failed and must
			// not write anything more.
			if err := w.WriteValues(columnNames, row); !errors.Is(err, errInjected) {
				t.Fatalf("WriteValues() after failure error = %v, want sticky errInjected", err)
			}
			if err := w.Flush(); !errors.Is(err, errInjected) {
				t.Fatalf("Flush() after failure error = %v, want sticky errInjected", err)
			}
			if out.buf.Len() != lenAfterFailure {
				t.Fatalf("output grew after write failure: %q", out.buf.String())
			}
		})
	}
}

// TestSQLInsertWriterBatchedWriteFailureFlushRegression reproduces #204: with a
// fail-then-recover sink, a write error inside a multi-row batch used to leave
// a half-written tuple, and a later Flush appended ";\n" and returned nil,
// ending the stream with invalid SQL under a success result. Now statements are
// emitted whole and the error is latched: output contains only complete
// statements and Flush returns the first error.
func TestSQLInsertWriterBatchedWriteFailureFlushRegression(t *testing.T) {
	t.Parallel()

	out := &failNthWrite{n: 2}
	w := mustNewSQLInsertWriter(t, out, "users", WithSQLBatchSize(2))
	columnNames := []string{"id"}
	row := func(id int64) []spanner.GenericColumnValue {
		return []spanner.GenericColumnValue{gcvctor.Int64Value(id)}
	}

	// Rows 1-2 close the first batch on the size boundary: Write call 1 succeeds.
	for id := int64(1); id <= 2; id++ {
		if err := w.WriteValues(columnNames, row(id)); err != nil {
			t.Fatalf("WriteValues(%d) error = %v", id, err)
		}
	}
	// Row 3 buffers; row 4 closes the second batch: Write call 2 fails.
	if err := w.WriteValues(columnNames, row(3)); err != nil {
		t.Fatalf("WriteValues(3) error = %v", err)
	}
	if err := w.WriteValues(columnNames, row(4)); !errors.Is(err, errInjected) {
		t.Fatalf("WriteValues(4) error = %v, want errInjected", err)
	}

	// Regression: Flush previously returned nil here and appended ";\n" after a
	// half-written tuple. It must report the latched error and write nothing,
	// even though the sink has recovered.
	if err := w.Flush(); !errors.Is(err, errInjected) {
		t.Fatalf("Flush() after batched write failure error = %v, want errInjected", err)
	}
	if err := w.WriteValues(columnNames, row(5)); !errors.Is(err, errInjected) {
		t.Fatalf("WriteValues(5) error = %v, want sticky errInjected", err)
	}

	// Output stays whole-statement-granular: only the first complete statement.
	want := "INSERT INTO `users` (`id`) VALUES\n  (1),\n  (2);\n"
	if diff := cmp.Diff(want, out.buf.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}

func TestNewSQLInsertWriterRejectsOutOfRangeKind(t *testing.T) {
	t.Parallel()

	kinds := []SQLInsertKind{SQLInsertKind(-1), SQLInsertOrUpdate + 1, SQLInsertKind(99)}
	for _, kind := range kinds {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w, err := NewSQLInsertWriter(&out, "users", WithSQLInsertKind(kind))
			if !errors.Is(err, ErrInvalidSQLInsertKind) {
				t.Fatalf("NewSQLInsertWriter() error = %v, want ErrInvalidSQLInsertKind", err)
			}
			if w != nil {
				t.Fatal("NewSQLInsertWriter() returned non-nil writer with invalid kind")
			}
		})
	}
}

func TestDelimitedWriterFlushAfterLateHeaderEnable(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewCSVWriter(t, &out, WithHeader(false))
	if err := w.WriteValues([]string{"name"}, []spanner.GenericColumnValue{gcvctor.StringValue("Alice")}); err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}

	// The header opportunity has passed; Flush must not return
	// ErrHeaderAfterData and must not strand the buffered row.
	w.Header = true
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() after late Header enable error = %v", err)
	}
	want := "Alice\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}
