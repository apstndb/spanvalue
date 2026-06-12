package spanvalue

import (
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
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
// match must be non-nil. A nil [sppb.Type] falls through without calling
// match (the built-in handling classifies such values as malformed wire),
// so predicates need not be nil-safe. For a bare type-code guard, use
// [PluginForTypeCode]; compose with [PluginSkippingNull] when the body only
// handles non-NULL values.
func PluginForType(match func(*sppb.Type) bool, plugin FormatComplexFunc) FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if value.Type == nil || !match(value.Type) {
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

// PluginFromNullable lifts a [FormatNullableFunc] into the plugin chain:
// non-NULL scalar values are decoded to their [NullableValue] wrapper — the
// same Decode-based dispatch as the [FormatConfig.FormatNullable] slow path,
// including the PG-annotated wrappers ([cloud.google.com/go/spanner.PGNumeric],
// [cloud.google.com/go/spanner.PGJsonB]) — and formatted with f. ARRAY and
// STRUCT values, SQL NULLs, and type codes outside the scalar set fall
// through ([ErrFallthrough]).
//
// f itself may return [ErrFallthrough] to defer values it does not claim,
// so per-type overrides compose with [NullableFormatterFor] and the rest of
// the chain (preset scalar plugins, built-ins) keeps formatting everything
// the override leaves alone — no access to a preset's own FormatNullable
// function is needed:
//
//	cfg := spanvalue.SimpleFormatConfig().WithComplexPlugin(
//	    spanvalue.PluginFromNullable(spanvalue.NullableFormatterFor(
//	        func(v spanner.NullNumeric) (string, error) {
//	            return "NUMERIC:" + v.Numeric.FloatString(2), nil
//	        })))
//
// Like every complex plugin it also runs for ARRAY elements, so an override
// applies inside ARRAY<T> as well. Decode failures other than the
// unsupported-type-code class are returned as real errors.
func PluginFromNullable(f FormatNullableFunc) FormatComplexFunc {
	return func(_ Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		if isComplexType(value.Type.GetCode()) || IsNull(value) {
			return "", ErrFallthrough
		}
		nv, err := simpleGCVToNullable(value)
		if errors.Is(err, ErrUnknownType) {
			// An unsupported type code is a coverage question for the rest
			// of the chain, not this plugin's error to raise.
			return "", ErrFallthrough
		}
		if err != nil {
			return "", err
		}
		return f(nv)
	}
}

// NullableFormatterFor restricts a typed formatter to the single
// [NullableValue] wrapper type T, deferring every other value with
// [ErrFallthrough]. It is meant for composition through
// [PluginFromNullable]; assigned directly to [FormatConfig.FormatNullable]
// the deferral surfaces as an error, because only the plugin chain
// interprets [ErrFallthrough].
func NullableFormatterFor[T NullableValue](f func(T) (string, error)) FormatNullableFunc {
	return func(v NullableValue) (string, error) {
		if tv, ok := v.(T); ok {
			return f(tv)
		}
		return "", ErrFallthrough
	}
}
