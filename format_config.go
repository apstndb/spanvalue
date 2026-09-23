package spanvalue

import "slices"

// Validate reports invalid hand-built [FormatConfig] values. Preset constructors and
// [NewFormatConfig] return configs that pass [*FormatConfig.Validate]. Nil fc returns
// [ErrNilFormatConfig].
//
// Static checks: non-empty NullString ([ErrEmptyNullString]; empty is rejected so NULL
// output is explicit, not ambiguous with an empty STRING), a non-empty
// FormatComplexPlugins chain ([ErrEmptyFormatComplexPlugins]), and non-nil chain
// elements ([ErrNilFormatComplexPlugin]).
//
// Validate cannot inspect what a plugin claims, so it does not prove that the chain
// covers every type: coverage is a runtime property — a non-NULL value that every
// plugin defers fails with [ErrUnhandledValue] at format time. [NewFormatConfig]
// additionally requires the canonical ARRAY/STRUCT/scalar handlers at build time.
func (fc *FormatConfig) Validate() error {
	if fc == nil {
		return ErrNilFormatConfig
	}
	if fc.NullString == "" {
		return ErrEmptyNullString
	}
	if len(fc.FormatComplexPlugins) == 0 {
		return ErrEmptyFormatComplexPlugins
	}
	for _, plugin := range fc.FormatComplexPlugins {
		if plugin == nil {
			return ErrNilFormatComplexPlugin
		}
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
