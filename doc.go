// Package spanvalue formats Cloud Spanner data from the Go client
// (cloud.google.com/go/spanner): [cloud.google.com/go/spanner.GenericColumnValue] values for individual columns
// and `*spanner.Row` values for full rows ([cloud.google.com/go/spanner.Row]), into strings for SQL literals, JSON,
// Spanner CLI–compatible text, and related styles.
//
// # Primary API
//
// Configure output with [FormatConfig]. Use the constructors [LiteralFormatConfig],
// [SimpleFormatConfig], [SpannerCLICompatibleFormatConfig], and [JSONFormatConfig] to pick
// a preset. Scalar plugins ([FormatSimpleValue], [FormatLiteralValue],
// [FormatSpannerCLIValue], [FormatJSONSimpleValue]) format GenericColumnValue directly
// without Decode; remove them with [FormatConfigWithoutScalarPlugins] or from
// [FormatConfig.FormatComplexPlugins] to use Decode + [FormatNullable].
// Scalar plugins fall through to that path when [FormatConfig.FormatNullable] is replaced.
// Constructors return a new [FormatConfig]; use [FormatConfig.Clone] when sharing a config.
// For tuple STRUCT with Spanner CLI scalars, set [FormatTupleStruct] on
// [SpannerCLICompatibleFormatConfig] (see README).
// [FormatConfig.FormatColumn] runs [FormatComplexFunc] plugins first, then built-in
// ARRAY, STRUCT, and scalar formatting.
// Convenience entry points include [FormatRowLiteral], [FormatColumnLiteral],
// [FormatRowJSONObject], and [FormatRowSpannerCLICompatible]. Identifier quoting helpers are
// [QuoteIdentifier] and [QuoteQualifiedIdentifier].
//
// # Advanced extension API
//
// Lower-level callbacks and plugin types are intended for custom output formats:
// [FormatArrayFunc], [FormatStructFieldFunc], [FormatStructParenFunc], [FormatComplexFunc],
// [ErrFallthrough], [FormatStruct], [FormatTupleStruct], [TypedStructFormat], and
// [JSONObjectStructFormat]. Customize a [FormatConfig] from a constructor, or
// [FormatConfig.Clone] when reusing one. Convenience formatters such as
// [FormatRowSpannerCLICompatible] use internal singleton configs; call
// [FormatConfig.FormatRow] on your own config instead of those helpers.
//
// # Related packages
//
// To build [cloud.google.com/go/spanner.GenericColumnValue] values from Go types, see
// [github.com/apstndb/spanvalue/gcvctor]. For streaming row export, see
// [github.com/apstndb/spanvalue/writer].
package spanvalue
