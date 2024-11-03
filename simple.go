package spanvalue

var (
	_ FormatNullableFunc = formatNullableValueSimple
)

var SimpleFormatConfig = FormatConfig{
	NullString:  nullStringClientLib,
	FormatArray: FormatUntypedArray,
	FormatStruct: FormatStruct{
		FormatStructField: FormatTypelessStructField,
		FormatStructParen: FormatTupleStruct,
	},
	FormatNullable: formatNullableValueSimple,
}

func formatNullableValueSimple(value NullableValue) (string, error) {
	return value.String(), nil
}
