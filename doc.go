// Package spanvalue formats Cloud Spanner data from the Go client
// (cloud.google.com/go/spanner): [cloud.google.com/go/spanner.GenericColumnValue] values for individual columns
// and `*spanner.Row` values for full rows ([cloud.google.com/go/spanner.Row]), into strings for SQL literals, JSON,
// Spanner CLI–compatible text, and related styles.
//
// Configure output with [FormatConfig]. Use the constructors [LiteralFormatConfig],
// [SimpleFormatConfig], [SpannerCLICompatibleFormatConfig], and [JSONFormatConfig] to pick
// a preset. [FormatConfig.FormatColumn] runs [FormatComplexFunc] plugins first, then
// built-in ARRAY, STRUCT, and scalar formatting. Convenience entry points include
// [FormatRowLiteral], [FormatColumnLiteral], [FormatRowJSONObject], and
// [FormatRowSpannerCLICompatible].
//
// To build [cloud.google.com/go/spanner.GenericColumnValue] values from Go types, see the sibling package
// [github.com/apstndb/spanvalue/gcvctor].
package spanvalue
