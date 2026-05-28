// Package spanvalue formats Cloud Spanner data from the Go client
// (cloud.google.com/go/spanner): [cloud.google.com/go/spanner.GenericColumnValue] values for individual columns
// and `*spanner.Row` values for full rows ([cloud.google.com/go/spanner.Row]), into strings for SQL literals, JSON,
// Spanner CLI–compatible text, and related styles.
//
// # Primary API
//
// Configure output with [FormatConfig]. Use the constructors [LiteralFormatConfig],
// [SimpleFormatConfig], [SpannerCLICompatibleFormatConfig], and [JSONFormatConfig] to pick
// a preset. Customize a preset with [FormatConfig.Clone]. [FormatConfig.FormatColumn] runs
// [FormatComplexFunc] plugins first, then built-in ARRAY, STRUCT, and scalar formatting.
// Convenience entry points include [FormatRowLiteral], [FormatColumnLiteral],
// [FormatRowJSONObject], and [FormatRowSpannerCLICompatible]. Identifier quoting helpers are
// [QuoteIdentifier] and [QuoteQualifiedIdentifier].
//
// # Advanced extension API
//
// Lower-level callbacks and plugin types are intended for custom output formats:
// [FormatArrayFunc], [FormatStructFieldFunc], [FormatStructParenFunc], [FormatComplexFunc],
// [ErrFallthrough], [FormatStruct], [FormatTupleStruct], [FormatTypedStruct], and
// [FormatJSONObjectStruct]. Copy a preset with [FormatConfig.Clone] before changing
// callbacks; do not mutate shared instances returned by convenience formatters.
//
// # Related packages
//
// To build [cloud.google.com/go/spanner.GenericColumnValue] values from Go types, see
// [github.com/apstndb/spanvalue/gcvctor]. For streaming row export, see
// [github.com/apstndb/spanvalue/writer].
package spanvalue
