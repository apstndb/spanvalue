package spanvalue

import (
	"errors"
)

var (
	// ErrScalarFormatterRequired is returned by [NewFormatConfig] when no
	// [WithScalarFormatter] option was supplied (or its formatter was nil).
	ErrScalarFormatterRequired = errors.New("scalar formatter required")
	// ErrArrayFormatRequired is returned by [NewFormatConfig] when no
	// [WithArrayFormat] option was supplied (or its join function was nil).
	ErrArrayFormatRequired = errors.New("array format required")
	// ErrStructFormatRequired is returned by [NewFormatConfig] when no
	// [WithStructFormat] option was supplied (or either of its callbacks was nil).
	ErrStructFormatRequired = errors.New("struct format required")
)

// formatConfigBuilder collects [FormatConfigOption] state for [NewFormatConfig].
type formatConfigBuilder struct {
	nullString string
	// plugins is the override region, most recent registration first.
	plugins     []FormatComplexFunc
	arrayJoin   FormatArrayFunc
	structField FormatStructFieldFunc
	structParen FormatStructParenFunc
	scalar      FormatNullableFunc
}

// FormatConfigOption configures [NewFormatConfig].
type FormatConfigOption func(*formatConfigBuilder)

// WithNullString sets [FormatConfig.NullString], the string every NULL value
// renders as unless a [WithPlugin] override claims it first. Required:
// [NewFormatConfig] rejects an empty NullString with [ErrEmptyNullString]
// (same rule as [*FormatConfig.Validate]).
func WithNullString(s string) FormatConfigOption {
	return func(b *formatConfigBuilder) { b.nullString = s }
}

// WithPlugin prepends p to the override region of the plugin chain: the most
// recently registered plugin runs first, matching
// [*FormatConfig.WithComplexPlugin] semantics. Overrides run before the
// [WithArrayFormat] / [WithStructFormat] / [WithScalarFormatter] handlers, so
// they can claim any value (including SQL NULLs and typed NULL arrays) or
// defer with [ErrFallthrough]. A nil p fails [NewFormatConfig] with
// [ErrNilFormatComplexPlugin].
//
// WithPlugin alone never satisfies the required handler options: even a
// plugin that covers every value must be accompanied by WithArrayFormat,
// WithStructFormat, and WithScalarFormatter. Partial, plugin-only configs are
// built with plain [FormatConfig] struct literals today.
func WithPlugin(p FormatComplexFunc) FormatConfigOption {
	return func(b *formatConfigBuilder) {
		b.plugins = append([]FormatComplexFunc{p}, b.plugins...)
	}
}

// WithArrayFormat sets the non-NULL ARRAY handler, installed as
// [PluginForArray](join) after the [WithPlugin] overrides. NULL ARRAY values
// render as the [WithNullString] value unless an override claims them.
// Required; a nil join is treated as unset ([ErrArrayFormatRequired]).
func WithArrayFormat(join FormatArrayFunc) FormatConfigOption {
	return func(b *formatConfigBuilder) { b.arrayJoin = join }
}

// WithStructFormat sets the non-NULL STRUCT handlers, installed as
// [PluginForStruct](field, paren) after the [PluginForArray] handler. NULL
// STRUCT values render as the [WithNullString] value unless an override
// claims them. Required; a nil field or paren is treated as unset
// ([ErrStructFormatRequired]).
func WithStructFormat(field FormatStructFieldFunc, paren FormatStructParenFunc) FormatConfigOption {
	return func(b *formatConfigBuilder) {
		b.structField = field
		b.structParen = paren
	}
}

// WithScalarFormatter sets the non-NULL scalar handler, installed as the
// chain tail [PluginFromNullable](f). NULL scalars render as the
// [WithNullString] value unless an override claims them. Required; a nil f is
// treated as unset ([ErrScalarFormatterRequired]).
func WithScalarFormatter(f FormatNullableFunc) FormatConfigOption {
	return func(b *formatConfigBuilder) { b.scalar = f }
}

// NewFormatConfig assembles a [FormatConfig] from the plugin combinators with
// build-time validation. The chain is built in canonical order:
//
//  1. [WithPlugin] overrides, most recent registration first,
//  2. [PluginForArray] from [WithArrayFormat],
//  3. [PluginForStruct] from [WithStructFormat],
//  4. [PluginFromNullable] from [WithScalarFormatter] as the tail.
//
// NULL values that no override claims render as the [WithNullString] value.
// The returned config passes [*FormatConfig.Validate].
//
// Build-time validation: [ErrScalarFormatterRequired], [ErrArrayFormatRequired],
// and [ErrStructFormatRequired] when the respective handler option is missing;
// [ErrEmptyNullString] when [WithNullString] is missing or empty;
// [ErrNilFormatComplexPlugin] when a [WithPlugin] override is nil. Validation
// does not prove total coverage: a scalar type code outside the
// [PluginFromNullable] domain that no override claims surfaces
// [ErrUnhandledValue] at format time. Nil options are ignored.
func NewFormatConfig(opts ...FormatConfigOption) (*FormatConfig, error) {
	var b formatConfigBuilder
	for _, opt := range opts {
		if opt != nil {
			opt(&b)
		}
	}
	if b.scalar == nil {
		return nil, ErrScalarFormatterRequired
	}
	if b.arrayJoin == nil {
		return nil, ErrArrayFormatRequired
	}
	if b.structField == nil || b.structParen == nil {
		return nil, ErrStructFormatRequired
	}
	plugins := make([]FormatComplexFunc, 0, len(b.plugins)+3)
	plugins = append(plugins, b.plugins...)
	plugins = append(plugins,
		PluginForArray(b.arrayJoin),
		PluginForStruct(b.structField, b.structParen),
		PluginFromNullable(b.scalar),
	)
	fc := &FormatConfig{
		NullString:           b.nullString,
		FormatComplexPlugins: plugins,
	}
	if err := fc.Validate(); err != nil {
		return nil, err
	}
	return fc, nil
}
