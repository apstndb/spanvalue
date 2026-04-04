// Package gcvctor constructs [cloud.google.com/go/spanner.GenericColumnValue] values from Go values and
// explicit [cloud.google.com/go/spanner/apiv1/spannerpb.Type] metadata, using [github.com/apstndb/spantype/typector] for type shapes.
//
// [ArrayValue] infers the element type from the first element (or uses a default empty ARRAY<INT64>
// when len==0, whether the variadic slice is nil or empty). [ArrayValueWithType] takes the element type
// explicitly; len==0 yields an empty ARRAY<elemType>. For a SQL NULL ARRAY, use [TypedNull] with
// [github.com/apstndb/spantype/typector.ElemTypeToArrayType] or [github.com/apstndb/spantype/typector.ElemCodeToArrayType] instead of relying
// on variadic nil. [StructValue] pairs field
// names with values; counts must match.
//
// ARRAY-typed [cloud.google.com/go/spanner/apiv1/spannerpb.Type] values require array_element_type
// (protobuf: array_element_type; Go field name ArrayElementType); omitting it yields an invalid ARRAY
// shape and Spanner may reject the request.
//
// Zero-argument [ArrayValue] returns an empty ARRAY<INT64> with complete type metadata in the Type field
// of the [cloud.google.com/go/spanner.GenericColumnValue]. For empty arrays, callers typically must supply
// explicit SQL type information through these constructors or through the ParamTypes field (protobuf:
// param_types) on [cloud.google.com/go/spanner/apiv1/spannerpb.ExecuteSqlRequest], because an empty list value
// does not specify an element type by itself.
//
// [TypedNull] returns a typed NULL for any [cloud.google.com/go/spanner/apiv1/spannerpb.Type], including STRUCT and ARRAY; the
// [cloud.google.com/go/spanner.GenericColumnValue] Value field is always a scalar protobuf null at the top level. [SimpleTypedNull] does the same for simple
// scalar type codes only—it cannot express STRUCT field layouts or ARRAY element types.
// Neither encodes a non-null STRUCT whose fields are all null; use [StructValue] with
// per-field nulls when you need that shape.
//
// Formatting these values as strings is provided by the sibling package
// [github.com/apstndb/spanvalue].
package gcvctor
