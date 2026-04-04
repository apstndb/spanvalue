// Package gcvctor constructs [spanner.GenericColumnValue] values from Go values and
// explicit [sppb.Type] metadata, using [github.com/apstndb/spantype/typector] for type shapes.
//
// [ArrayValue] infers the element type from the first element (or uses a default empty
// ARRAY<INT64> when called with no arguments). [ArrayValueWithType] takes the element type
// explicitly. [StructValue] pairs field names with values; counts must match.
//
// [TypedNull] and [SimpleTypedNull] always set the top-level protobuf value to a scalar
// null ([structpb.NullValue]) for every Spanner type, including STRUCT and ARRAY—they do
// not build a non-null STRUCT whose fields are all null; use [StructValue] with
// per-field nulls when you need that shape.
//
// Formatting these values as strings is provided by the sibling package
// [github.com/apstndb/spanvalue].
package gcvctor
