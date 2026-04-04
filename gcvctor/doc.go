// Package gcvctor constructs [cloud.google.com/go/spanner.GenericColumnValue] values from Go values and
// explicit [cloud.google.com/go/spanner/apiv1/spannerpb.Type] metadata, using [github.com/apstndb/spantype/typector] for type shapes.
//
// [ArrayValue] infers the element type from the first element when arguments are non-empty.
// With no arguments, or with ArrayValue(nil...), it returns a typed NULL ARRAY<INT64> ([ArrayCodeTypedNull]).
// With a non-nil empty slice (ArrayValue([]GenericColumnValue{}...)), it returns a non-null empty ARRAY<INT64>.
// [ArrayValueWithType] takes the element type explicitly and follows the same variadic rule: nil elems
// yields a typed NULL ARRAY<elemType> ([ArrayTypeTypedNull]); a non-nil empty slice yields an empty
// non-null array. [StructValue] pairs field names with values; counts must match.
//
// [TypedNull] returns a typed NULL for any [cloud.google.com/go/spanner/apiv1/spannerpb.Type], including STRUCT and ARRAY; the
// stored Value is always a scalar protobuf null. [SimpleTypedNull] does the same for simple
// scalar type codes only—it cannot express STRUCT field layouts or ARRAY element types.
// Neither encodes a non-null STRUCT whose fields are all null; use [StructValue] with
// per-field nulls when you need that shape.
//
// Formatting these values as strings is provided by the sibling package
// [github.com/apstndb/spanvalue].
package gcvctor
