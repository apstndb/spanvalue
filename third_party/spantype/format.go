// Package spantype formats Cloud Spanner google.spanner.v1.Type values with
// multiple verbosity levels for logs, errors, and debugging output.
package spantype

import (
	"fmt"
	"strconv"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// StructMode controls how STRUCT types are rendered by selecting one of
// StructModeBase, StructModeRecursive, or StructModeRecursiveWithName.
type StructMode int

const (
	// StructModeBase formats `STRUCT` type as `STRUCT`.
	StructModeBase StructMode = iota
	// StructModeRecursive formats `STRUCT` type with field types. e.g. `STRUCT<INT64, STRUCT<INT64>>`
	StructModeRecursive
	// StructModeRecursiveWithName formats `STRUCT` type with field types with field name. e.g. `STRUCT<n INT64, s STRUCT<n INT64>>`
	StructModeRecursiveWithName
)

// ProtoEnumMode controls how PROTO and ENUM types are rendered by selecting
// base, leaf, full-name, or explicit-kind output.
type ProtoEnumMode int

const (
	// ProtoEnumModeBase formats `PROTO` and `ENUM` type as `PROTO` and `ENUM`.
	ProtoEnumModeBase ProtoEnumMode = iota
	// ProtoEnumModeLeaf formats `PROTO` and `ENUM` type without package name. e.g. `ProtoType`, `EnumType`
	ProtoEnumModeLeaf
	// ProtoEnumModeFull formats `PROTO` and `ENUM` type as full qualified name. e.g. `examples.ProtoType`, `examples.EnumType`
	ProtoEnumModeFull
	// ProtoEnumModeLeafWithKind formats `PROTO` and `ENUM` type without package name with kind.
	// e.g. `PROTO<ProtoType>`, `ENUM<EnumType>`
	ProtoEnumModeLeafWithKind
	// ProtoEnumModeFullWithKind formats `PROTO` and `ENUM` type as full qualified name with kind.
	// e.g. `PROTO<examples.ProtoType>`, `ENUM<examples.EnumType>`.
	// Note: It should be same format with `INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE`.
	ProtoEnumModeFullWithKind
)

// ArrayMode controls how ARRAY types are rendered by selecting either base or
// recursive element rendering.
type ArrayMode int

const (
	// ArrayModeBase formats `ARRAY` type as `ARRAY`
	ArrayModeBase ArrayMode = iota
	// ArrayModeRecursive formats `ARRAY` type with element type. e.g. `ARRAY<INT64>`
	ArrayModeRecursive
)

// UnknownMode controls how unknown type codes are rendered by selecting
// UNKNOWN, the raw numeric code, a verbose form, or panic behavior.
type UnknownMode int

const (
	// UnknownModeUnknown formats unknown type code as `UNKNOWN`
	UnknownModeUnknown UnknownMode = iota
	// UnknownModeTypeCode formats unknown type code as e.g. `-1`
	UnknownModeTypeCode
	// UnknownModeVerbose formats unknown type code as `UNKNOWN(int32(code))` as e.g. `UNKNOWN(-1)`
	UnknownModeVerbose
	// UnknownModePanic panics when type code is unknown.
	UnknownModePanic
)

// FormatOption is an option for FormatType, and FormatStructFields.
type FormatOption struct {
	// Struct controls STRUCT formatting.
	Struct  StructMode
	// Proto controls PROTO formatting.
	Proto   ProtoEnumMode
	// Enum controls ENUM formatting.
	Enum    ProtoEnumMode
	// Array controls ARRAY formatting.
	Array   ArrayMode
	// Unknown controls formatting for unknown type codes.
	Unknown UnknownMode
}

var (
	// FormatOptionSimplest is a FormatOption for FormatTypeSimplest.
	FormatOptionSimplest = FormatOption{
		Struct:  StructModeBase,
		Proto:   ProtoEnumModeBase,
		Enum:    ProtoEnumModeBase,
		Array:   ArrayModeBase,
		Unknown: UnknownModeTypeCode,
	}
	// FormatOptionSimple is a FormatOption for FormatTypeSimple.
	FormatOptionSimple = FormatOption{
		Struct:  StructModeBase,
		Proto:   ProtoEnumModeLeaf,
		Enum:    ProtoEnumModeLeaf,
		Array:   ArrayModeRecursive,
		Unknown: UnknownModeUnknown,
	}
	// FormatOptionNormal is a FormatOption for FormatTypeNormal.
	FormatOptionNormal = FormatOption{
		Struct:  StructModeRecursive,
		Proto:   ProtoEnumModeLeaf,
		Enum:    ProtoEnumModeLeaf,
		Array:   ArrayModeRecursive,
		Unknown: UnknownModeVerbose,
	}
	// FormatOptionVerbose is a FormatOption for FormatTypeVerbose.
	FormatOptionVerbose = FormatOption{
		Struct:  StructModeRecursiveWithName,
		Proto:   ProtoEnumModeFull,
		Enum:    ProtoEnumModeFull,
		Array:   ArrayModeRecursive,
		Unknown: UnknownModeVerbose,
	}
	// FormatOptionMoreVerbose is a FormatOption for FormatTypeMoreVerbose.
	FormatOptionMoreVerbose = FormatOption{
		Struct:  StructModeRecursiveWithName,
		Proto:   ProtoEnumModeFullWithKind,
		Enum:    ProtoEnumModeFullWithKind,
		Array:   ArrayModeRecursive,
		Unknown: UnknownModeVerbose,
	}
)

func lastCut(s, sep string) (before string, after string, found bool) {
	if i := strings.LastIndex(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return "", s, false
}

func formatTypeAnnotationSuffix(ann sppb.TypeAnnotationCode) string {
	if ann == sppb.TypeAnnotationCode_TYPE_ANNOTATION_CODE_UNSPECIFIED {
		return ""
	}
	return "(" + ann.String() + ")"
}

// FormatType formats Cloud Spanner type using the given FormatOption.
// When [sppb.Type.TypeAnnotation] is set (e.g. PostgreSQL PG_NUMERIC / PG_JSONB), it is appended
// as a parenthetical suffix on the formatted type, e.g. `NUMERIC(PG_NUMERIC)`.
func FormatType(typ *sppb.Type, opts FormatOption) string {
	return formatTypeImpl(typ, opts) + formatTypeAnnotationSuffix(typ.GetTypeAnnotation())
}

func formatTypeImpl(typ *sppb.Type, opts FormatOption) string {
	code := typ.GetCode()
	switch code {
	case sppb.TypeCode_ARRAY:
		if opts.Array == ArrayModeBase {
			break
		}
		return fmt.Sprintf("ARRAY<%v>", FormatType(typ.GetArrayElementType(), opts))
	case sppb.TypeCode_PROTO:
		return FormatProtoEnum(typ, opts.Proto)
	case sppb.TypeCode_ENUM:
		return FormatProtoEnum(typ, opts.Enum)
	case sppb.TypeCode_STRUCT:
		if opts.Struct == StructModeBase {
			break
		}
		return fmt.Sprintf("STRUCT<%v>", FormatStructFields(typ.GetStructType().GetFields(), opts))
	}

	return FormatTypeCode(code, opts.Unknown)
}

// FormatProtoEnum formats `PROTO` or `ENUM` type using ProtoEnumMode.
// It panics when the input type is not `PROTO` or `ENUM`.
func FormatProtoEnum(typ *sppb.Type, mode ProtoEnumMode) string {
	if typ.GetCode() != sppb.TypeCode_PROTO && typ.GetCode() != sppb.TypeCode_ENUM {
		panic(fmt.Sprintf("precondition failed: TypeCode must be PROTO or ENUM, but %v", typ))
	}

	switch mode {
	case ProtoEnumModeLeaf:
		_, after, _ := lastCut(typ.GetProtoTypeFqn(), ".")
		return after
	case ProtoEnumModeFull:
		return typ.GetProtoTypeFqn()
	case ProtoEnumModeLeafWithKind:
		_, after, _ := lastCut(typ.GetProtoTypeFqn(), ".")
		return fmt.Sprintf("%v<%v>", typ.GetCode().String(), after)
	case ProtoEnumModeFullWithKind:
		return fmt.Sprintf("%v<%v>", typ.GetCode().String(), typ.GetProtoTypeFqn())
	default:
		return typ.GetCode().String()
	}
}

// FormatTypeCode formats sppb.TypeCode, but it formats unknown type code as `UNKNOWN(int32(code))`. e.g. `UNKNOWN(-1)`
func FormatTypeCode(code sppb.TypeCode, mode UnknownMode) string {
	if name, ok := sppb.TypeCode_name[int32(code)]; ok {
		return name
	}
	switch mode {
	case UnknownModeTypeCode:
		return strconv.Itoa(int(code))
	case UnknownModeVerbose:
		return fmt.Sprintf("UNKNOWN(%v)", int32(code))
	case UnknownModePanic:
		panic(fmt.Sprintf("precondition failed: unknown TypeCode(%v)", int32(code)))
	default:
		return "UNKNOWN"
	}
}

// FormatStructFields formats Cloud Spanner struct fields or `metadata.rowType` using the given FormatOption.
func FormatStructFields(fields []*sppb.StructType_Field, opts FormatOption) string {
	var fieldsStr []string
	for _, field := range fields {
		typeStr := FormatType(field.GetType(), opts)
		if opts.Struct == StructModeRecursiveWithName && field.GetName() != "" {
			fieldsStr = append(fieldsStr, fmt.Sprintf("%v %v", field.GetName(), typeStr))
		} else {
			fieldsStr = append(fieldsStr, typeStr)
		}
	}
	return strings.Join(fieldsStr, ", ")
}

// FormatTypeSimplest formats Cloud Spanner type as simplest format.
// e.g. `INT64`, `ARRAY`, `PROTO`, `ENUM`, `STRUCT`
func FormatTypeSimplest(typ *sppb.Type) string {
	return FormatType(typ, FormatOptionSimplest)
}

// FormatTypeSimple formats Cloud Spanner type as simple format.
// e.g. `INT64`, `ARRAY<INT64>`, `ProtoType`, `EnumType`, `STRUCT`
func FormatTypeSimple(typ *sppb.Type) string {
	return FormatType(typ, FormatOptionSimple)
}

// FormatTypeNormal formats Cloud Spanner type as normal format.
// e.g. `INT64`, `ARRAY<INT64>`, `ProtoType`, `EnumType`, `STRUCT<INT64>`
func FormatTypeNormal(typ *sppb.Type) string {
	return FormatType(typ, FormatOptionNormal)
}

// FormatTypeVerbose formats Cloud Spanner type as verbose format.
// e.g. `INT64`, `ARRAY<INT64>`, `examples.ProtoType`, `examples.EnumType`, `STRUCT<n INT64>`
func FormatTypeVerbose(typ *sppb.Type) string {
	return FormatType(typ, FormatOptionVerbose)
}

// FormatTypeMoreVerbose formats Cloud Spanner type as more verbose format.
// e.g. `INT64`, `ARRAY<INT64>`, `PROTO<examples.ProtoType>`, `ENUM<examples.EnumType>`, `STRUCT<n INT64>`
func FormatTypeMoreVerbose(typ *sppb.Type) string {
	return FormatType(typ, FormatOptionMoreVerbose)
}
