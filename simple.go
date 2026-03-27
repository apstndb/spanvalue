package spanvalue

var (
	_ FormatNullableFunc = formatNullableValueSimple
)

// SimpleFormatConfig returns a new FormatConfig that produces human-readable
// output using client library conventions.
func SimpleFormatConfig() *FormatConfig {
	return &FormatConfig{
		NullString:  nullStringClientLib,
		FormatArray: FormatUntypedArray,
		FormatStruct: FormatStruct{
			FormatStructField: FormatTypelessStructField,
			FormatStructParen: FormatTupleStruct,
		},
		FormatNullable: formatNullableValueSimple,
	}
}

func formatNullableValueSimple(value NullableValue) (string, error) {
	return value.String(), nil
}
