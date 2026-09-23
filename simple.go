package spanvalue

var (
	_ FormatNullableFunc = formatNullableValueSimple
)

// SimpleFormatConfig returns a new FormatConfig that produces human-readable
// output using client library conventions. The chain is [FormatSimpleValue]
// for scalars, [PluginForArray] with [FormatUntypedArray], and
// [PluginForStruct] with [FormatTypelessStructField] and [FormatTupleStruct].
func SimpleFormatConfig() *FormatConfig {
	return &FormatConfig{
		NullString: nullStringClientLib,
		FormatComplexPlugins: []FormatComplexFunc{
			FormatSimpleValue,
			PluginForArray(FormatUntypedArray),
			PluginForStruct(FormatTypelessStructField, FormatTupleStruct),
		},
	}
}

func formatNullableValueSimple(value NullableValue) (string, error) {
	return value.String(), nil
}
