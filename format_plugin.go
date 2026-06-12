package spanvalue

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner"
)

// PluginForType restricts plugin to values whose [sppb.Type] satisfies match;
// every other value falls through ([ErrFallthrough]) to the rest of the
// plugin chain and the built-in formatting. It lifts the type-guard
// boilerplate out of [FormatComplexFunc] authors:
//
//	PluginForType(func(t *sppb.Type) bool {
//	    return t.GetCode() == sppb.TypeCode_JSON &&
//	        t.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB
//	}, body)
//
// match must be non-nil. For a bare type-code guard, use [PluginForTypeCode];
// compose with [PluginSkippingNull] when the body only handles non-NULL
// values.
func PluginForType(match func(*sppb.Type) bool, plugin FormatComplexFunc) FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if !match(value.Type) {
			return "", ErrFallthrough
		}
		return plugin(formatter, value, toplevel)
	}
}

// PluginForTypeCode is [PluginForType] matching on the type code alone.
// Note that annotated types share a code with their GoogleSQL base (PG_JSONB
// is TypeCode_JSON, PG_NUMERIC is TypeCode_NUMERIC); use [PluginForType] with
// a predicate when the annotation matters.
func PluginForTypeCode(code sppb.TypeCode, plugin FormatComplexFunc) FormatComplexFunc {
	return PluginForType(func(t *sppb.Type) bool { return t.GetCode() == code }, plugin)
}

// PluginSkippingNull makes SQL NULL values fall through ([ErrFallthrough])
// so plugin only sees non-NULL values. The chain's built-in scalar handling
// renders NULL via [Formatter.GetNullString] on every preset, so deferring is
// output-equivalent to returning the null string from the plugin itself —
// unless a later plugin in the chain claims NULLs of the same type.
func PluginSkippingNull(plugin FormatComplexFunc) FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if IsNull(value) {
			return "", ErrFallthrough
		}
		return plugin(formatter, value, toplevel)
	}
}
