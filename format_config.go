package spanvalue

import "slices"

// Validate reports invalid hand-built [FormatConfig] values. Preset constructors and
// [NewFormatConfig] return configs that pass [*FormatConfig.Validate]. Nil fc returns
// [ErrNilFormatConfig].
//
// Static checks: non-empty NullString (empty is rejected so NULL output is explicit,
// not ambiguous with an empty STRING) and non-nil elements in FormatComplexPlugins.
//
// The deprecated FormatArray, FormatStruct, and FormatNullable fields may each be nil
// when FormatComplexPlugins is non-empty: plugins may cover those value shapes — configs
// built by [NewFormatConfig] cover non-NULL ARRAY, STRUCT, and scalar values via
// [PluginForArray], [PluginForStruct], and [PluginFromNullable], so the nil fields are
// never invoked (NULL values render as NullString before any field would run). Validate
// cannot inspect what a plugin claims, so this relaxation does not prove coverage: a
// value that every plugin defers reaches the built-in path, where a nil FormatArray or
// FormatStruct callback panics and a nil FormatNullable returns
// [ErrFormatNullableRequired]. When FormatComplexPlugins is empty, the field checks
// remain static: nil FormatArray ([ErrNilFormatArray]), nil FormatStruct callbacks
// ([ErrNilFormatStructField], [ErrNilFormatStructParen]), and nil FormatNullable
// ([ErrFormatNullableRequired], because non-NULL scalars have no formatter; runtime
// behavior is defined in #163).
func (fc *FormatConfig) Validate() error {
	if fc == nil {
		return ErrNilFormatConfig
	}
	if fc.NullString == "" {
		return ErrEmptyNullString
	}
	for _, plugin := range fc.FormatComplexPlugins {
		if plugin == nil {
			return ErrNilFormatComplexPlugin
		}
	}
	if len(fc.FormatComplexPlugins) > 0 {
		// Plugins may cover the ARRAY/STRUCT/scalar shapes (see doc comment);
		// the deprecated fields are then optional.
		return nil
	}
	if fc.FormatArray == nil {
		return ErrNilFormatArray
	}
	if fc.FormatStruct.FormatStructField == nil {
		return ErrNilFormatStructField
	}
	if fc.FormatStruct.FormatStructParen == nil {
		return ErrNilFormatStructParen
	}
	if fc.FormatNullable == nil {
		return ErrFormatNullableRequired
	}
	return nil
}

// WithComplexPlugin returns a clone of fc with plugin prepended to FormatComplexPlugins
// so it runs before existing plugins (including preset scalar plugins). This matches the
// protofmt pattern of prepending descriptor-aware plugins before preset defaults.
// The original config, including shared preset singletons, is not mutated. Chain further
// calls on the returned config for additional plugins (each prepends, so the most recent
// call runs first). Nil fc returns nil. A nil plugin panics so a mistaken nil in a chain
// fails at the call site instead of collapsing the chain to nil.
func (fc *FormatConfig) WithComplexPlugin(plugin FormatComplexFunc) *FormatConfig {
	if fc == nil {
		return nil
	}
	if plugin == nil {
		panic("spanvalue: WithComplexPlugin: nil plugin")
	}
	clone := fc.Clone()
	clone.FormatComplexPlugins = append([]FormatComplexFunc{plugin}, clone.FormatComplexPlugins...)
	return clone
}

// Clone returns a shallow copy of fc with a copied FormatComplexPlugins slice.
// The returned config is independent for field assignment and plugin list
// mutation; callback values themselves are shared with the source.
// Clone returns nil when fc is nil.
func (fc *FormatConfig) Clone() *FormatConfig {
	if fc == nil {
		return nil
	}
	clone := *fc
	if fc.FormatComplexPlugins != nil {
		clone.FormatComplexPlugins = slices.Clone(fc.FormatComplexPlugins)
	}
	return &clone
}
