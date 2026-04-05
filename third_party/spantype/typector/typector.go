// Package typector provides small constructor helpers for building Cloud
// Spanner google.spanner.v1.Type values and struct fields in tests and callers.
package typector

import (
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// Shorthand constructors for all simple Spanner types.
// Each call returns a new *sppb.Type to prevent shared mutation across callers.
// PROTO and ENUM are excluded because they require a fully qualified name.

// Bool returns a BOOL type.
func Bool() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_BOOL) }

// Int64 returns an INT64 type.
func Int64() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_INT64) }

// Float32 returns a FLOAT32 type.
func Float32() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_FLOAT32) }

// Float64 returns a FLOAT64 type.
func Float64() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_FLOAT64) }

// Timestamp returns a TIMESTAMP type.
func Timestamp() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_TIMESTAMP) }

// Date returns a DATE type.
func Date() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_DATE) }

// String returns a STRING type.
func String() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_STRING) }

// Bytes returns a BYTES type.
func Bytes() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_BYTES) }

// Numeric returns a NUMERIC type.
func Numeric() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_NUMERIC) }

// JSON returns a JSON type.
func JSON() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_JSON) }

// Interval returns an INTERVAL type.
func Interval() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_INTERVAL) }

// UUID returns a UUID type.
func UUID() *sppb.Type { return CodeToSimpleType(sppb.TypeCode_UUID) }

// CodeToSimpleType returns a simple non-container type for the given code.
func CodeToSimpleType(code sppb.TypeCode) *sppb.Type {
	return &sppb.Type{Code: code}
}

// SimpleTypeWithAnnotation returns a simple type with the given code and optional
// [sppb.Type.TypeAnnotation] (use [sppb.TypeAnnotationCode_TYPE_ANNOTATION_CODE_UNSPECIFIED] for none).
func SimpleTypeWithAnnotation(code sppb.TypeCode, ann sppb.TypeAnnotationCode) *sppb.Type {
	return &sppb.Type{Code: code, TypeAnnotation: ann}
}

// PGNumeric returns a NUMERIC type with PostgreSQL [sppb.TypeAnnotationCode_PG_NUMERIC] semantics.
func PGNumeric() *sppb.Type {
	return SimpleTypeWithAnnotation(sppb.TypeCode_NUMERIC, sppb.TypeAnnotationCode_PG_NUMERIC)
}

// PGJsonB returns a JSON type with PostgreSQL [sppb.TypeAnnotationCode_PG_JSONB] semantics.
func PGJsonB() *sppb.Type {
	return SimpleTypeWithAnnotation(sppb.TypeCode_JSON, sppb.TypeAnnotationCode_PG_JSONB)
}

// ElemCodeToArrayType returns an ARRAY type with the given element type code.
func ElemCodeToArrayType(code sppb.TypeCode) *sppb.Type {
	return ElemTypeToArrayType(CodeToSimpleType(code))
}

// ElemTypeToArrayType returns an ARRAY type with the given element type.
func ElemTypeToArrayType(typ *sppb.Type) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: typ}
}

// StructTypeFieldsToStructType returns a STRUCT type with the given fields.
func StructTypeFieldsToStructType(fields []*sppb.StructType_Field) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{Fields: fields}}
}

// FQNToProtoType returns a PROTO type for the given fully-qualified name.
func FQNToProtoType(fqn string) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: fqn}
}

// FQNToEnumType returns an ENUM type for the given fully-qualified name.
func FQNToEnumType(fqn string) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: fqn}
}

// NameCodeToStructType returns a single-field STRUCT type from a field name and type code.
func NameCodeToStructType(name string, code sppb.TypeCode) *sppb.Type {
	return NameTypeToStructType(name, CodeToSimpleType(code))
}

// NameTypeToStructType returns a single-field STRUCT type from a field name and type.
func NameTypeToStructType(name string, typ *sppb.Type) *sppb.Type {
	return StructTypeFieldsToStructType([]*sppb.StructType_Field{
		NameTypeToStructTypeField(name, typ),
	})
}

// NameCodeToStructTypeField returns a STRUCT field from a field name and type code.
func NameCodeToStructTypeField(name string, code sppb.TypeCode) *sppb.StructType_Field {
	return NameTypeToStructTypeField(name, CodeToSimpleType(code))
}

// NameTypeToStructTypeField returns a STRUCT field from a field name and type.
func NameTypeToStructTypeField(name string, typ *sppb.Type) *sppb.StructType_Field {
	return &sppb.StructType_Field{Name: name, Type: typ}
}

// CodeToUnnamedStructTypeField returns an unnamed STRUCT field for the given type code.
func CodeToUnnamedStructTypeField(code sppb.TypeCode) *sppb.StructType_Field {
	return NameTypeToStructTypeField("", CodeToSimpleType(code))
}

// TypeToUnnamedStructTypeField returns an unnamed STRUCT field for the given type.
func TypeToUnnamedStructTypeField(typ *sppb.Type) *sppb.StructType_Field {
	return &sppb.StructType_Field{Type: typ}
}

// NameTypeSlicesToStructType returns a STRUCT type from parallel slices of field names and types.
func NameTypeSlicesToStructType(names []string, types []*sppb.Type) (*sppb.Type, error) {
	fields, err := NameTypeSlicesToStructTypeFields(names, types)
	if err != nil {
		return nil, err
	}
	return StructTypeFieldsToStructType(fields), nil
}

// MustNameTypeSlicesToStructType is like NameTypeSlicesToStructType but panics on error.
func MustNameTypeSlicesToStructType(names []string, types []*sppb.Type) *sppb.Type {
	return must(NameTypeSlicesToStructType(names, types))
}

// NameTypeSlicesToStructTypeFields returns STRUCT fields from parallel slices of field names and types.
func NameTypeSlicesToStructTypeFields(names []string, types []*sppb.Type) ([]*sppb.StructType_Field, error) {
	if len(names) != len(types) {
		return nil, fmt.Errorf("length mismatch: len(names)=%d, len(types)=%d", len(names), len(types))
	}

	var fields []*sppb.StructType_Field
	for i := range names {
		fields = append(fields, NameTypeToStructTypeField(names[i], types[i]))
	}
	return fields, nil
}

// MustNameTypeSlicesToStructTypeFields is like NameTypeSlicesToStructTypeFields but panics on error.
func MustNameTypeSlicesToStructTypeFields(names []string, types []*sppb.Type) []*sppb.StructType_Field {
	return must(NameTypeSlicesToStructTypeFields(names, types))
}

// NameCodeSlicesToStructType returns a STRUCT type from parallel slices of field names and type codes.
func NameCodeSlicesToStructType(names []string, codes []sppb.TypeCode) (*sppb.Type, error) {
	fields, err := NameCodeSlicesToStructTypeFields(names, codes)
	if err != nil {
		return nil, err
	}
	return StructTypeFieldsToStructType(fields), nil
}

// MustNameCodeSlicesToStructType is like NameCodeSlicesToStructType but panics on error.
func MustNameCodeSlicesToStructType(names []string, codes []sppb.TypeCode) *sppb.Type {
	return must(NameCodeSlicesToStructType(names, codes))
}

// NameCodeSlicesToStructTypeFields returns STRUCT fields from parallel slices of field names and type codes.
func NameCodeSlicesToStructTypeFields(names []string, codes []sppb.TypeCode) ([]*sppb.StructType_Field, error) {
	var types []*sppb.Type
	for _, code := range codes {
		types = append(types, CodeToSimpleType(code))
	}

	return NameTypeSlicesToStructTypeFields(names, types)
}

// MustNameCodeSlicesToStructTypeFields is like NameCodeSlicesToStructTypeFields but panics on error.
func MustNameCodeSlicesToStructTypeFields(names []string, codes []sppb.TypeCode) []*sppb.StructType_Field {
	return must(NameCodeSlicesToStructTypeFields(names, codes))
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
