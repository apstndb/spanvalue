// Package gcvctor constructs [cloud.google.com/go/spanner.GenericColumnValue] values from Go values and
// explicit [cloud.google.com/go/spanner/apiv1/spannerpb.Type] metadata, using [github.com/apstndb/spantype/typector] for type shapes.
//
// [ArrayValue] infers the element type from the first element (or uses a default empty
// ARRAY<INT64> when called with no arguments). [ArrayValueWithType] takes the element type
// explicitly. [StructValue] pairs field names with values; counts must match.
//
// Spanner expects well-formed [cloud.google.com/go/spanner/apiv1/spannerpb.Type] metadata for
// ARRAY (including array_element_type). A disposable test harness comparing emulator vs real
// behavior for malformed ARRAY types lives in the repository at research/arraytypeverify (not
// part of this Go module).
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
