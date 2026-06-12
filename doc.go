// Package spanvalue formats Cloud Spanner data from the Go client
// (cloud.google.com/go/spanner): [cloud.google.com/go/spanner.GenericColumnValue] values for individual columns
// and `*spanner.Row` values for full rows ([cloud.google.com/go/spanner.Row]), into strings for SQL literals, JSON,
// Spanner CLI–compatible text, and related styles.
//
// # Primary API
//
// Configure output with [FormatConfig], which holds exactly two fields: the
// NULL rendering ([FormatConfig.NullString]) and the ordered plugin chain
// ([FormatConfig.FormatComplexPlugins]). Use the constructors
// [LiteralFormatConfig], [LiteralFormatConfigWithQuote],
// [LiteralFormatConfigWithSingleQuotedLiterals], [LiteralFormatConfigWithOptions],
// [SimpleFormatConfig], [SpannerCLICompatibleFormatConfig], and
// [JSONFormatConfig] to pick a preset. Literal quote options
// ([LiteralQuoteConfig], [WithLiteralQuote]) are captured into the literal
// preset's plugins at construction time.
//
// [FormatConfig.FormatColumn] tries each [FormatComplexFunc] plugin in order;
// a plugin returns [ErrFallthrough] to defer. When every plugin defers, NULL
// values of any type render as [FormatConfig.NullString], and non-NULL values
// fail with [ErrUnhandledValue] — chain coverage is a runtime property.
// Constructors return a new [FormatConfig]; call [FormatConfig.Clone] or
// [FormatConfig.WithComplexPlugin] (prepends a plugin, so the most recent
// addition runs first) before mutating a config you may reuse. After
// hand-assembling a config, call [FormatConfig.Validate] to catch an empty
// [FormatConfig.NullString], an empty chain, or nil plugins before the first
// [FormatConfig.FormatRow] call. Package writer does not call Validate on
// [writer.WithFormatter] configs; validate hand-built formatters before
// passing them to writers.
//
// Convenience entry points include [FormatRowLiteral], [FormatColumnLiteral],
// [FormatRowJSONObject], and [FormatRowSpannerCLICompatible]; they use internal
// singleton configs, so call [FormatConfig.FormatRow] on your own config when
// customizing. Identifier quoting helpers are [QuoteIdentifier] and
// [QuoteQualifiedIdentifier].
//
// # Customization: builder and plugins
//
// [NewFormatConfig] assembles a config from canonical handlers with
// build-time validation: [WithPlugin] overrides (most recent first), then
// [WithArrayFormat] ([PluginForArray]), [WithStructFormat]
// ([PluginForStruct]), and the [WithScalarFormatter] tail
// ([PluginFromNullable]). Missing handlers fail at construction
// ([ErrScalarFormatterRequired], [ErrArrayFormatRequired],
// [ErrStructFormatRequired]) instead of on the first row.
//
// To customize a preset, prepend plugins with [FormatConfig.WithComplexPlugin]:
// a prepended plugin runs before the preset handlers, so it can override any
// type (for tuple STRUCT with Spanner CLI scalars, prepend
// [PluginForStruct]([FormatSimpleStructField], [FormatTupleStruct]); see the
// README). For per-scalar-type overrides use [PluginForNullable] (the
// pre-composed [PluginFromNullable] + [NullableFormatterFor] form); values
// the override defers keep the preset behavior.
// A prepended [PluginFromNullable] with a total formatter replaces preset
// scalar formatting wholesale.
//
// Plugin authors lift the usual type and NULL guards with [PluginForType],
// [PluginForTypeCode], and [PluginSkippingNull]. Callback types are
// [FormatArrayFunc], [FormatStructFieldFunc] (Formatter-based),
// [FormatStructParenFunc], [FormatNullableFunc], and [FormatComplexFunc];
// exported building blocks include [FormatTupleStruct], [FormatTypedStruct],
// [FormatBracketStruct], [FormatUntypedArray], [FormatOptionallyTypedArray],
// [FormatCompactArray], and [NewJSONObjectStructFormatter].
//
// # Related packages
//
// To build [cloud.google.com/go/spanner.GenericColumnValue] values from Go types, see
// [github.com/apstndb/spanvalue/gcvctor]. For streaming row export, see
// [github.com/apstndb/spanvalue/writer]. For opt-in descriptor-aware PROTO and ENUM
// display plugins, see [github.com/apstndb/spanvalue/protofmt].
package spanvalue
