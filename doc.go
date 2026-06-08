// Package spanvalue formats Cloud Spanner data from the Go client
// (cloud.google.com/go/spanner): [cloud.google.com/go/spanner.GenericColumnValue] values for individual columns
// and `*spanner.Row` values for full rows ([cloud.google.com/go/spanner.Row]), into strings for SQL literals, JSON,
// Spanner CLI–compatible text, and related styles.
//
// # Primary API
//
// Configure output with [FormatConfig]. Use the constructors [LiteralFormatConfig],
// [LiteralFormatConfigWithQuote], [LiteralFormatConfigWithSingleQuotedLiterals],
// [LiteralFormatConfigWithOptions], [SimpleFormatConfig], [SpannerCLICompatibleFormatConfig],
// and [JSONFormatConfig] to pick a preset. [WithLiteralQuote] sets [FormatConfig].Literal.Quote
// on the literal preset (string/bytes delimiter policy for SQL-style literals).
// After hand-assembling a [FormatConfig], call [*FormatConfig.Validate] to catch nil callbacks
// or an empty [FormatConfig.NullString] before formatting rows.
// Scalar plugins ([FormatSimpleValue], [FormatLiteralValue],
// [FormatSpannerCLIValue], [FormatJSONSimpleValue]) format GenericColumnValue directly
// without Decode; remove them with [FormatConfigWithoutScalarPlugins] or from
// [FormatConfig.FormatComplexPlugins] to use Decode + [FormatConfig.FormatNullable]
// (set FormatNullable on the clone; nil returns [ErrFormatNullableRequired]).
// Scalar plugins fall through to that path when [FormatConfig.FormatNullable] is set.
// Constructors return a new [FormatConfig]; call [*FormatConfig.Clone] or
// [*FormatConfig.WithComplexPlugin] before mutating a config you may reuse
// ([*FormatConfig.Clone] copies [FormatConfig.FormatComplexPlugins]).
// For tuple STRUCT with Spanner CLI scalars, clone [SpannerCLICompatibleFormatConfig]
// and set [FormatTupleStruct] (see README).
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
// [github.com/apstndb/spanvalue/writer]. For opt-in descriptor-aware PROTO and ENUM
// display plugins, see [github.com/apstndb/spanvalue/protofmt].
package spanvalue
