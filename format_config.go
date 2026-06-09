package spanvalue

import "slices"

// Validate reports invalid hand-built [FormatConfig] values. Preset constructors return
// configs that pass [*FormatConfig.Validate]. Nil fc returns [ErrNilFormatConfig].
//
// Static checks: non-empty NullString (empty is rejected so NULL output is explicit,
// not ambiguous with an empty STRING); non-nil FormatArray and FormatStruct callback
// fields; non-nil elements in FormatComplexPlugins. The FormatNullable field may be nil
// when a preset scalar plugin is present in FormatComplexPlugins; when scalar plugins are
// absent, a nil FormatNullable field fails validation because non-NULL scalars have no
// formatter (runtime behavior is defined in #163). Validate does not prove that
// plugin-only configs format every type. Only preset scalar plugins satisfy the
// FormatNullable exemption; custom scalar plugins in FormatComplexPlugins are not
// detected, so keep FormatNullable non-nil to pass Validate when using custom plugins.
func (fc *FormatConfig) Validate() error {
	if fc == nil {
		return ErrNilFormatConfig
	}
	if fc.NullString == "" {
		return ErrEmptyNullString
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
	for _, plugin := range fc.FormatComplexPlugins {
		if plugin == nil {
			return ErrNilFormatComplexPlugin
		}
	}
	if fc.FormatNullable == nil && !hasPresetScalarPlugin(fc) {
		return ErrFormatNullableRequired
	}
	return nil
}

func hasPresetScalarPlugin(fc *FormatConfig) bool {
	return slices.ContainsFunc(fc.FormatComplexPlugins, isPresetScalarPlugin)
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
