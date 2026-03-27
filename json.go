package spanvalue

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// JSONFormatConfig produces valid JSON value strings for each Spanner value.
// Each formatted string is a standalone JSON value:
//   - NULL → null
//   - BOOL → true / false
//   - INT64 → 42 (unquoted number)
//   - FLOAT32/FLOAT64 → 3.14 (NaN/Inf as quoted strings)
//   - ENUM → 42 (unquoted number, Spanner stores proto enum values as INT64)
//   - STRING, BYTES, TIMESTAMP, DATE, NUMERIC, PROTO, INTERVAL, UUID → "quoted string"
//   - JSON column → raw JSON value (passed through)
//   - ARRAY → [elem1,elem2,...]
//   - STRUCT → {"field1":val1,"field2":val2,...}
var JSONFormatConfig = &FormatConfig{
	NullString:  "null",
	FormatArray: FormatCompactArray,
	FormatStruct: FormatStruct{
		FormatStructField: FormatSimpleStructField,
		FormatStructParen: FormatJSONObjectStruct,
	},
	FormatComplexPlugins: []FormatComplexFunc{
		FormatJSONSimpleValue,
	},
	FormatNullable: FormatNullableSpannerCLICompatible,
}

// FormatRowJSONObject formats a spanner.Row as a single JSON object string
// using the given FormatConfig for value formatting and column names as keys.
// The FormatConfig must produce standalone JSON values per column (e.g.,
// JSONFormatConfig). Using a non-JSON config produces syntactically invalid output.
// Empty column names (e.g., from expressions without aliases like SELECT 1+1)
// are assigned names by the provided namer function. If namer is nil, empty
// names are kept as empty-string JSON keys.
// Output: {"col1":val1,"col2":val2,...}
func FormatRowJSONObject(fc *FormatConfig, row *spanner.Row, namer UnnamedFieldNamer) (string, error) {
	values, err := fc.FormatRow(row)
	if err != nil {
		return "", err
	}
	return assembleJSONObject(row.ColumnNames(), values, namer), nil
}

// assembleJSONObject combines column names and pre-formatted JSON value strings
// into a single JSON object. Empty names are resolved using the namer function
// (if non-nil), with collision avoidance against explicit and previously
// generated names. If namer is nil, empty names are kept as-is.
func assembleJSONObject(columnNames []string, values []string, namer UnnamedFieldNamer) string {
	// Collect all explicit names for collision avoidance.
	usedNames := make(map[string]bool, len(columnNames))
	for _, name := range columnNames {
		if name != "" {
			usedNames[name] = true
		}
	}

	var b strings.Builder
	b.WriteByte('{')
	autoIdx := 0
	for i, val := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		var name string
		if i < len(columnNames) {
			name = columnNames[i]
		}
		if name == "" && namer != nil {
			// Find a unique name. Detect pathological namers that cycle
			// without producing new candidates by tracking seen names.
			// This panic indicates a bug in the namer (contract violation).
			attempted := make(map[string]bool)
			for {
				name = namer(autoIdx)
				autoIdx++
				if !usedNames[name] {
					break
				}
				if attempted[name] {
					panic(fmt.Sprintf("bug in UnnamedFieldNamer: returned repeated colliding name %q", name))
				}
				attempted[name] = true
			}
			usedNames[name] = true
		}
		// json.Marshal on a Go string never returns an error.
		// Note: strconv.Quote is not suitable here because it produces Go string
		// literal escapes (e.g., \a, \v) that are not valid JSON escape sequences.
		key, _ := json.Marshal(name) //nolint:errcheck // string marshal is infallible
		b.Write(key)
		b.WriteByte(':')
		b.WriteString(val)
	}
	b.WriteByte('}')
	return b.String()
}

// FormatCompactArray formats array elements without spaces between separators.
// Output: [elem1,elem2,elem3]
func FormatCompactArray(_ *sppb.Type, _ bool, elemStrings []string) string {
	return "[" + strings.Join(elemStrings, ",") + "]"
}

// UnnamedFieldNamer generates a name for an unnamed field or column.
// The index argument is a monotonically increasing counter (not necessarily the
// field's positional index) that may skip values due to collision avoidance.
// It must return distinct non-empty names for distinct indices.
// Functions that accept UnnamedFieldNamer (such as NewJSONObjectStructFormatter
// and FormatRowJSONObject) panic if the namer violates this contract.
// Pass nil instead of a namer to keep unnamed fields as empty-string keys.
type UnnamedFieldNamer func(index int) string

// IndexedUnnamedFieldNamer produces names like "_0", "_1", etc.
// The underscore prefix minimizes collision with user-defined names.
// Suitable for row columns (e.g., SELECT 1+1 produces "_0").
func IndexedUnnamedFieldNamer(index int) string {
	return "_" + strconv.Itoa(index)
}

// FormatJSONObjectStruct formats struct fields as a JSON object with nil namer.
// Unnamed struct fields produce empty-string keys, matching Spanner's own representation.
var FormatJSONObjectStruct = NewJSONObjectStructFormatter(nil)

// NewJSONObjectStructFormatter creates a FormatStructParenFunc that formats struct fields
// as a JSON object with field names as keys. Unnamed fields are assigned names by the
// provided namer function. If namer is nil, unnamed fields keep empty-string keys
// (which produces duplicate keys — valid per RFC 8259 but may cause issues with
// parsers that deduplicate keys).
// Panics if a non-nil namer returns the same name for different indices (contract violation).
// Output: {"field1":val1,"field2":val2,...}
func NewJSONObjectStructFormatter(namer UnnamedFieldNamer) FormatStructParenFunc {
	return func(typ *sppb.Type, _ bool, fieldStrings []string) string {
		fields := typ.GetStructType().GetFields()
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.GetName()
		}
		return assembleJSONObject(names, fieldStrings, namer)
	}
}

// FormatJSONSimpleValue is a FormatComplexFunc that formats all non-ARRAY, non-STRUCT
// types as valid JSON values. It never returns ErrFallthrough.
//
// For most types, structpb.Value.MarshalJSON() produces the correct JSON representation
// (BOOL→true/false, FLOAT→number, STRING→"quoted", NULL→null, NaN/Inf→"NaN"/"Infinity").
// Only INT64, ENUM, and JSON columns need special handling:
//   - INT64: Spanner encodes as StringValue("42"), MarshalJSON() would produce "42" (quoted),
//     but we want 42 (unquoted number).
//   - ENUM: Spanner stores proto enum values as INT64; same handling as INT64.
//   - JSON: Spanner encodes as StringValue('{"key":"value"}'), MarshalJSON() would produce
//     escaped quoted string, but we want the raw JSON value passed through.
func FormatJSONSimpleValue(_ Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	val := value.Value

	// Handle NULL uniformly for all types. This is technically redundant for
	// the default case (MarshalJSON handles NULL), but ensures correctness
	// regardless of how switch cases evolve.
	// Note: protobuf generated getters are nil-receiver safe, so val.GetKind()
	// does not panic even if val is nil.
	if _, isNull := val.GetKind().(*structpb.Value_NullValue); isNull {
		return "null", nil
	}

	switch value.Type.GetCode() {
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM, sppb.TypeCode_JSON:
		// INT64: StringValue is already a valid JSON number
		// ENUM: Spanner stores proto enum values as INT64; StringValue is a valid JSON number
		// JSON column: StringValue is already valid JSON
		return val.GetStringValue(), nil

	default:
		// For all other types, structpb.Value's JSON marshaling matches our needs
		b, err := val.MarshalJSON()
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
