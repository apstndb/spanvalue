package writer

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanvalue"
)

// DelimitedGCVExportOptions returns [WithMetadata], [WithFormatter], and
// [WithUnnamedFieldNamer] for delimited writers that stream [DelimitedWriter.WriteGCVs].
func DelimitedGCVExportOptions(
	metadata *sppb.ResultSetMetadata,
	formatter *spanvalue.FormatConfig,
	namer spanvalue.UnnamedFieldNamer,
) []DelimitedOption {
	return []DelimitedOption{
		WithMetadata(metadata),
		WithFormatter(formatter),
		WithUnnamedFieldNamer(namer),
	}
}

// JSONLGCVExportOptions returns the same trio for [NewJSONLWriter] and [JSONLWriter.WriteGCVs].
func JSONLGCVExportOptions(
	metadata *sppb.ResultSetMetadata,
	formatter *spanvalue.FormatConfig,
	namer spanvalue.UnnamedFieldNamer,
) []JSONLOption {
	return []JSONLOption{
		WithMetadata(metadata),
		WithFormatter(formatter),
		WithUnnamedFieldNamer(namer),
	}
}
