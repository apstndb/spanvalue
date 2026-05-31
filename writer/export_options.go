package writer

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanvalue"
)

// DelimitedGCVExportOptions returns [WithMetadata], [WithFormatter], and
// [WithUnnamedFieldNamer] for delimited writers that stream [DelimitedWriter.WriteGCVs].
// Nil arguments are omitted so defaults are not overwritten accidentally.
func DelimitedGCVExportOptions(
	metadata *sppb.ResultSetMetadata,
	formatter *spanvalue.FormatConfig,
	namer spanvalue.UnnamedFieldNamer,
) []DelimitedOption {
	var opts []DelimitedOption
	if metadata != nil {
		opts = append(opts, WithMetadata(metadata))
	}
	if formatter != nil {
		opts = append(opts, WithFormatter(formatter))
	}
	if namer != nil {
		opts = append(opts, WithUnnamedFieldNamer(namer))
	}
	return opts
}

// JSONLGCVExportOptions returns the same trio for [NewJSONLWriter] and [JSONLWriter.WriteGCVs].
// Nil arguments are omitted so defaults are not overwritten accidentally.
func JSONLGCVExportOptions(
	metadata *sppb.ResultSetMetadata,
	formatter *spanvalue.FormatConfig,
	namer spanvalue.UnnamedFieldNamer,
) []JSONLOption {
	var opts []JSONLOption
	if metadata != nil {
		opts = append(opts, WithMetadata(metadata))
	}
	if formatter != nil {
		opts = append(opts, WithFormatter(formatter))
	}
	if namer != nil {
		opts = append(opts, WithUnnamedFieldNamer(namer))
	}
	return opts
}
