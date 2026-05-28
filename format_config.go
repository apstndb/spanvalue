package spanvalue

import "slices"

// Clone returns a shallow copy of fc with a copied FormatComplexPlugins slice.
// The returned config is independent for field assignment and plugin list
// mutation; callback values themselves are shared with the source.
// Clone returns nil when fc is nil.
func (fc *FormatConfig) Clone() *FormatConfig {
	if fc == nil {
		return nil
	}
	clone := *fc
	if len(fc.FormatComplexPlugins) > 0 {
		clone.FormatComplexPlugins = slices.Clone(fc.FormatComplexPlugins)
	}
	return &clone
}
