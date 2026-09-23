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

// PluginForArray lifts a [FormatArrayFunc] into the plugin chain: for
// non-NULL ARRAY values the wire list value is extracted (non-list payloads
// are [ErrUnexpectedComplexValueKind]), each element is recursively formatted
// with formatter.FormatColumn(elem, false) so the whole chain applies per
// element, and the element strings are handed to join. join must be non-nil.
//
// A nil Type, a non-ARRAY type code, and SQL NULL fall through
// ([ErrFallthrough]); NULL deferral lets the built-in handling render
// [Formatter.GetNullString]. Plugin authors who want typed NULL arrays — for
// example rendering CAST(NULL AS bigint[]) — should instead write a plain
// [PluginForTypeCode](ARRAY, ...) plugin, which receives NULL values.
func PluginForArray(join FormatArrayFunc) FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if value.Type == nil || value.Type.GetCode() != sppb.TypeCode_ARRAY || IsNull(value) {
			return "", ErrFallthrough
		}
		return formatArrayElems(formatter, value, toplevel, join)
	}
}

// PluginForStruct lifts STRUCT formatting into the plugin chain: for non-NULL
// STRUCT values the wire list value is extracted (non-list payloads are
// [ErrUnexpectedComplexValueKind]), the value count is checked against the
// field descriptors ([ErrMismatchedFields]), each field is formatted with the
// [FormatStructFieldFunc] callback (use formatter.FormatColumn(fieldGCV, false)
// to recurse into the field value), and the field strings are handed to paren.
// Both callbacks must be non-nil.
//
// A nil Type, a non-STRUCT type code, and SQL NULL fall through
// ([ErrFallthrough]); NULL deferral lets the built-in handling render
// [Formatter.GetNullString]. For typed NULL STRUCT rendering write a plain
// [PluginForTypeCode](STRUCT, ...) plugin, which receives NULL values.
func PluginForStruct(field FormatStructFieldFunc, paren FormatStructParenFunc) FormatComplexFunc {
	return func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
		if value.Type == nil || value.Type.GetCode() != sppb.TypeCode_STRUCT || IsNull(value) {
			return "", ErrFallthrough
		}
		return formatStructFields(formatter, value, toplevel, field, paren)
	}
}

// PluginFromNullable lifts a [FormatNullableFunc] into the plugin chain:
// non-NULL scalar values are decoded to their [NullableValue] wrapper —
// including the PG-annotated wrappers ([cloud.google.com/go/spanner.PGNumeric],
// [cloud.google.com/go/spanner.PGJsonB]) — and formatted with f. ARRAY and
// STRUCT values, SQL NULLs, and type codes outside the scalar set fall
// through ([ErrFallthrough]).
//
// f itself may return [ErrFallthrough] to defer values it does not claim,
// so per-type overrides compose with [NullableFormatterFor] and the rest of
// the chain (preset scalar plugins included) keeps formatting everything
// the override leaves alone:
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
// [PluginFromNullable], which lets the deferral reach the rest of the
// plugin chain.
func NullableFormatterFor[T NullableValue](f func(T) (string, error)) FormatNullableFunc {
	return func(v NullableValue) (string, error) {
		if tv, ok := v.(T); ok {
			return f(tv)
		}
		return "", ErrFallthrough
	}
}

// PluginForNullable is the pre-composed combination of [PluginFromNullable]
// and [NullableFormatterFor]: a chain plugin that
// formats exactly the scalar values decoding to the [NullableValue] wrapper
// type T and defers everything else — other scalar types, SQL NULL, ARRAY,
// STRUCT, and unsupported type codes all fall through ([ErrFallthrough]).
// Because the Decode dispatch is annotation-aware, T selects the dialect
// variant precisely: [cloud.google.com/go/spanner.PGNumeric] matches only
// PG_NUMERIC-annotated NUMERIC, [cloud.google.com/go/spanner.NullNumeric]
// only the GoogleSQL form.
//
//	cfg := spanvalue.SimpleFormatConfig().WithComplexPlugin(
//	    spanvalue.PluginForNullable(func(v spanner.NullNumeric) (string, error) {
//	        return v.Numeric.FloatString(2), nil
//	    }))
//
// Use the two primitives directly when one function should claim several
// wrapper types.
func PluginForNullable[T NullableValue](f func(T) (string, error)) FormatComplexFunc {
	return PluginFromNullable(NullableFormatterFor(f))
}
