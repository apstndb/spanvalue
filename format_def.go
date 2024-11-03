package spanvalue

var FormatTypedStruct = FormatStruct{
	FormatStructParen: formatTypedStructParen,
	FormatStructField: formatSimpleStructField,
}
