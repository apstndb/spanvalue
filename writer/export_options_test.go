package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestDelimitedGCVExportOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	md := metadataWithColumnNames("name")
	w := mustNewCSVWriter(t, &out, DelimitedGCVExportOptions(
		md,
		spanvalue.SimpleFormatConfig(),
		spanvalue.IndexedUnnamedFieldNamer,
	)...)
	if err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("x")}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name\nx\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedGCVExportOptionsSkipsNil(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewCSVWriter(t, &out, DelimitedGCVExportOptions(nil, nil, nil)...)
	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("x")})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestJSONLGCVExportOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	md := metadataWithColumnNames("name")
	w := mustNewJSONLWriter(t, &out, JSONLGCVExportOptions(
		md,
		spanvalue.JSONFormatConfig(),
		spanvalue.IndexedUnnamedFieldNamer,
	)...)
	if err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("x")}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := `{"name":"x"}` + "\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("JSONL output mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONLGCVExportOptionsSkipsNil(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewJSONLWriter(t, &out, JSONLGCVExportOptions(nil, nil, nil)...)
	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("x")})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}
