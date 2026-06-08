package gcvctor

import (
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// MustArrayValueOf is like [ArrayValueOf] but panics on error.
// Use only in tests and table-driven fixtures where schema and inputs are known good.
func MustArrayValueOf(elemType *sppb.Type, elems ...spanner.GenericColumnValue) spanner.GenericColumnValue {
	gcv, err := ArrayValueOf(elemType, elems...)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustStructValueOf is like [StructValueOf] but panics on error.
// Use only in tests and table-driven fixtures where schema and inputs are known good.
func MustStructValueOf(names []string, gcvs []spanner.GenericColumnValue) spanner.GenericColumnValue {
	gcv, err := StructValueOf(names, gcvs)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustStructValueOfFields is like [StructValueOfFields] but panics on error.
// Use only in tests and table-driven fixtures where schema and inputs are known good.
func MustStructValueOfFields(fields ...StructFieldValue) spanner.GenericColumnValue {
	gcv, err := StructValueOfFields(fields...)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustNormalizeArrayElements is like [NormalizeArrayElements] but panics on error.
// Use only in tests and table-driven fixtures where schema and inputs are known good.
func MustNormalizeArrayElements(elemType *sppb.Type, elems ...spanner.GenericColumnValue) []spanner.GenericColumnValue {
	normalized, err := NormalizeArrayElements(elemType, elems...)
	if err != nil {
		panic(err)
	}
	return normalized
}

// MustDateStringValue is like [DateStringValue] but panics on error.
// Use only in tests and table-driven fixtures where inputs are known good.
func MustDateStringValue(v string) spanner.GenericColumnValue {
	gcv, err := DateStringValue(v)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustTimestampStringValue is like [TimestampStringValue] but panics on error.
// Use only in tests and table-driven fixtures where inputs are known good.
func MustTimestampStringValue(v string) spanner.GenericColumnValue {
	gcv, err := TimestampStringValue(v)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustIntervalStringValue is like [IntervalStringValue] but panics on error.
// Use only in tests and table-driven fixtures where inputs are known good.
func MustIntervalStringValue(v string) spanner.GenericColumnValue {
	gcv, err := IntervalStringValue(v)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustJSONValue is like [JSONValue] but panics on error.
// Use only in tests and table-driven fixtures where inputs are known good.
func MustJSONValue(v any) spanner.GenericColumnValue {
	gcv, err := JSONValue(v)
	if err != nil {
		panic(err)
	}
	return gcv
}

// MustPGJSONBValue is like [PGJSONBValue] but panics on error.
// Use only in tests and table-driven fixtures where inputs are known good.
func MustPGJSONBValue(v any) spanner.GenericColumnValue {
	gcv, err := PGJSONBValue(v)
	if err != nil {
		panic(err)
	}
	return gcv
}
