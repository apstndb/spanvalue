package spanvalue

// TypedStructFormat returns a FormatStruct that formats STRUCT values with
// typed field names in literal style.
func TypedStructFormat() FormatStruct {
	return FormatStruct{
		FormatStructParen: formatTypedStructParen,
		FormatStructField: formatSimpleStructField,
	}
}
