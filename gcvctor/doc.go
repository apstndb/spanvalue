// Package gcvctor constructs [cloud.google.com/go/spanner.GenericColumnValue] values from Go values and
// explicit [cloud.google.com/go/spanner/apiv1/spannerpb.Type] metadata, using [github.com/apstndb/spantype/typector] for type shapes.
//
// [ArrayValue] infers the element type from the first element (or uses a default empty ARRAY<INT64>
// when len==0, whether the variadic slice is nil or empty). [ArrayValueOf] takes the element type
// explicitly; len==0 yields an empty ARRAY<elemType>. For a SQL NULL ARRAY, use [NullOf] with
// [github.com/apstndb/spantype/typector.ElemTypeToArrayType] or [github.com/apstndb/spantype/typector.ElemCodeToArrayType] instead of relying
// on variadic nil. [NormalizeArrayElements] rewrites SQL NULL elements to [NullOf] with elemType before
// a strict [ArrayValueOf] call when callers already know the final array element type. [StructValueOf] pairs field
// names with values; counts must match. [StructValueOfFields] accepts [StructField] pairs;
// [StructFieldKVOf] is the usual constructor; positional composite literals
// (StructField{name, value}) are also valid. Empty field names denote unnamed STRUCT fields.
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
// [NullOf] returns a typed NULL for any [cloud.google.com/go/spanner/apiv1/spannerpb.Type], including STRUCT and ARRAY; the
// [cloud.google.com/go/spanner.GenericColumnValue] Value field is always a scalar protobuf null at the top level. [NullFromCode] does the same for simple
// scalar type codes only—it cannot express STRUCT field layouts or ARRAY element types.
// [NullOf], [NullArrayOf], and [EmptyArrayOf] normalize a nil Type pointer input to
// TYPE_CODE_UNSPECIFIED so they never fabricate malformed nil Type pointers.
// Neither encodes a non-null STRUCT whose fields are all null; use [StructValueOf] with
// per-field nulls when you need that shape.
//
// Nullable Go inputs are split by shape in the function name:
//
//   - [BoolFromPtr], [Int64FromPtr], and related *FromPtr helpers take *T; nil means SQL NULL.
//   - [BytesFromSlice] takes []byte; nil means SQL NULL (slices are already reference types).
//   - [BoolFromNullable], [Int64FromNullable], and related *FromNullable helpers take
//     [cloud.google.com/go/spanner.NullBool], [cloud.google.com/go/spanner.NullInt64], and
//     other client null wrappers; Valid == false means SQL NULL.
//
// Use *FromPtr for optional fields modeled as Go pointers. Use *FromNullable when the value
// already comes from the Spanner client library. For explicit typed NULL without an input
// value, keep using [NullOf] or [NullFromCode].
//
// [NumericValueChecked] and [PGNumericValueChecked] return errors on nil [*big.Rat] input
// instead of panicking. The legacy [NumericValue] and [PGNumericValue] helpers keep their
// original signatures and return typed SQL NULL values on nil input.
//
// [PGNumericValue] and [PGJSONBValue] build PostgreSQL-dialect annotated NUMERIC/JSON values
// ([cloud.google.com/go/spanner/apiv1/spannerpb.TypeAnnotationCode_PG_NUMERIC],
// [cloud.google.com/go/spanner/apiv1/spannerpb.TypeAnnotationCode_PG_JSONB]).
//
// NUMERIC wire strings: [NumericValue] and [PGNumericValue] store Spanner-canonical decimals.
// [StringBasedValueFromCode] does not normalize; callers that build NUMERIC cells by hand must
// supply the same wire form Spanner returns (see that helper's doc). The [github.com/apstndb/spanvalue]
// formatters treat NUMERIC string payloads as authoritative and do not parse them again.
//
// Formatting these values as strings is provided by the sibling package
// [github.com/apstndb/spanvalue].
//
// # Test fixtures
//
// For nested ARRAY and STRUCT trees in tests, prefer [MustArrayValueOf], [MustStructValueOf],
// [MustStructValueOfFields], and [MustNormalizeArrayElements] over local panic-on-error helpers.
// They wrap the error-returning constructors and are intended for schema-known fixture data, not production paths.
//
// String payloads: [StringBasedValueFromCode] stores the wire string as-is with no validation
// (no extra imports beyond typector). When the test cares about canonical wire form, use validated
// helpers such as [DateStringValue] or the corresponding [MustDateStringValue], [MustTimestampStringValue],
// [MustIntervalStringValue], and [MustJSONValue] for inline nesting. Typed Go inputs ([DateValue] with
// [cloud.google.com/go/civil.Date], [TimestampValue] with [time.Time], and so on) avoid parse errors
// when you already hold the native value.
//
// See ExampleStringBasedValueFromCode_validatedDate and ExampleNormalizeArrayElements.
package gcvctor
