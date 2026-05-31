package writer

import (
	"bytes"
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
	w := NewCSVWriter(&out, DelimitedGCVExportOptions(
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
