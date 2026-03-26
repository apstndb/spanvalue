package spanvalue

import (
	"encoding/json"
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
// Empty column names (e.g., from expressions without aliases like SELECT 1+1)
// are assigned names by the provided namer function.
// Output: {"col1":val1,"col2":val2,...}
func FormatRowJSONObject(fc *FormatConfig, row *spanner.Row, namer UnnamedFieldNamer) (string, error) {
	values, err := fc.FormatRow(row)
	if err != nil {
		return "", err
	}
	return assembleJSONObject(row.ColumnNames(), values, namer), nil
}

// assembleJSONObject combines column names and pre-formatted JSON value strings
// into a single JSON object. Empty names are resolved using the namer function,
// with collision avoidance against explicit and previously generated names.
func assembleJSONObject(columnNames []string, values []string, namer UnnamedFieldNamer) string {
	if namer == nil {
		namer = IndexedUnnamedFieldNamer
	}

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
		if name == "" {
			for {
				name = namer(autoIdx)
				autoIdx++
				if name == "" || !usedNames[name] {
					break
				}
			}
			if name != "" {
				usedNames[name] = true
			}
		}
		keyJSON, _ := json.Marshal(name)
		b.Write(keyJSON)
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


// UnnamedFieldNamer generates a name for an unnamed struct field given its 0-based index.
type UnnamedFieldNamer func(index int) string

// IndexedUnnamedFieldNamer produces names like "_0", "_1", etc.
// The underscore prefix minimizes collision with user-defined names.
// Suitable for row columns (e.g., SELECT 1+1 produces "_0").
func IndexedUnnamedFieldNamer(index int) string {
	return "_" + strconv.Itoa(index)
}

// EmptyUnnamedFieldNamer always returns "" for unnamed fields.
// This produces duplicate empty-string keys when multiple unnamed fields exist,
// which is technically valid JSON (RFC 8259 does not forbid duplicate keys)
// but may cause issues with parsers that deduplicate keys.
func EmptyUnnamedFieldNamer(_ int) string {
	return ""
}

// FormatJSONObjectStruct formats struct fields as a JSON object using EmptyUnnamedFieldNamer.
// Unnamed struct fields produce empty-string keys, matching Spanner's own representation.
var FormatJSONObjectStruct = NewJSONObjectStructFormatter(EmptyUnnamedFieldNamer)

// NewJSONObjectStructFormatter creates a FormatStructParenFunc that formats struct fields
// as a JSON object with field names as keys. Unnamed fields are assigned names by the
// provided namer function, skipping names already used by explicit or previously generated
// fields to avoid duplicate JSON keys.
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
