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
//   - STRING, BYTES, TIMESTAMP, DATE, NUMERIC, ENUM, PROTO, INTERVAL, UUID → "quoted string"
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
// Output: {"col1":val1,"col2":val2,...}
func FormatRowJSONObject(fc *FormatConfig, row *spanner.Row) (string, error) {
	values, err := fc.FormatRow(row)
	if err != nil {
		return "", err
	}
	return assembleJSONObject(row.ColumnNames(), values), nil
}

func assembleJSONObject(columnNames []string, values []string) string {
	parts := make([]string, len(values))
	for i, val := range values {
		var name string
		if i < len(columnNames) {
			name = columnNames[i]
		}
		keyJSON, _ := json.Marshal(name)
		parts[i] = string(keyJSON) + ":" + val
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// FormatCompactArray formats array elements without spaces between separators.
// Output: [elem1,elem2,elem3]
func FormatCompactArray(_ *sppb.Type, _ bool, elemStrings []string) string {
	return "[" + strings.Join(elemStrings, ",") + "]"
}


// UnnamedFieldNamer generates a name for an unnamed struct field given its 0-based index.
type UnnamedFieldNamer func(index int) string

// DefaultUnnamedFieldNamer produces names like "_0", "_1", etc.
// The underscore prefix is chosen to minimize collision with user-defined column names.
func DefaultUnnamedFieldNamer(index int) string {
	return "_" + strconv.Itoa(index)
}

// EmptyUnnamedFieldNamer always returns "" for unnamed fields.
// This produces duplicate empty-string keys when multiple unnamed fields exist,
// which is technically valid JSON (RFC 8259 does not forbid duplicate keys)
// but may cause issues with parsers that deduplicate keys.
func EmptyUnnamedFieldNamer(_ int) string {
	return ""
}

// FormatJSONObjectStruct formats struct fields as a JSON object using DefaultUnnamedFieldNamer.
var FormatJSONObjectStruct = NewJSONObjectStructFormatter(DefaultUnnamedFieldNamer)

// NewJSONObjectStructFormatter creates a FormatStructParenFunc that formats struct fields
// as a JSON object with field names as keys. Unnamed fields are assigned names by the
// provided namer function, skipping names already used by other fields to avoid duplicate keys.
// Output: {"field1":val1,"field2":val2,...}
func NewJSONObjectStructFormatter(namer UnnamedFieldNamer) FormatStructParenFunc {
	return func(typ *sppb.Type, _ bool, fieldStrings []string) string {
		fields := typ.GetStructType().GetFields()

		// Collect all explicitly named fields to avoid collisions with auto-generated names.
		usedNames := make(map[string]bool, len(fields))
		for _, f := range fields {
			if f.GetName() != "" {
				usedNames[f.GetName()] = true
			}
		}

		parts := make([]string, len(fieldStrings))
		autoIdx := 0
		for i, valStr := range fieldStrings {
			name := fields[i].GetName()
			if name == "" {
				// Find a name that doesn't collide with any explicit field name.
				for {
					name = namer(autoIdx)
					autoIdx++
					if !usedNames[name] {
						break
					}
				}
			}
			keyJSON, _ := json.Marshal(name)
			parts[i] = string(keyJSON) + ":" + valStr
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
}

// FormatJSONSimpleValue is a FormatComplexFunc that formats all non-ARRAY, non-STRUCT
// types as valid JSON values. It never returns ErrFallthrough.
//
// For most types, structpb.Value.MarshalJSON() produces the correct JSON representation
// (BOOL→true/false, FLOAT→number, STRING→"quoted", NULL→null, NaN/Inf→"NaN"/"Infinity").
// Only INT64 and JSON columns need special handling:
//   - INT64: Spanner encodes as StringValue("42"), MarshalJSON() would produce "42" (quoted),
//     but we want 42 (unquoted number).
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
