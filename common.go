package spanvalue

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/lox"
	"github.com/ngicks/go-iterator-helper/hiter"
	"github.com/ngicks/go-iterator-helper/x/exp/xiter"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/internal"
)

// TODO: PGNumeric, PBJsonB,
const (
	nullStringUpperCase = "NULL"
	nullStringClientLib = "<null>"
)

type NullableValue interface {
	spanner.NullableValue
	fmt.Stringer
}

type NullBytes []byte

func (n NullBytes) IsNull() bool {
	return n == nil
}

func (n NullBytes) String() string {
	if n == nil {
		return nullStringClientLib
	}
	return strings.Join(lox.MapWithoutIndex(n, internal.ByteToEscapeSequenceReadable), "")
}

var _, _ NullableValue = (NullBytes)(nil), (*NullBytes)(nil)
var _, _ NullableValue = spanner.NullString{}, (*spanner.NullString)(nil)
var _, _ NullableValue = spanner.NullDate{}, (*spanner.NullDate)(nil)

// FormatComplexFunc is a function to format spanner.GenericColumnValue.
// If it returns ErrFallthrough, value will pass through to next step.
type FormatComplexFunc = func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error)

var ErrFallthrough = errors.New("fallthrough")

func typeValueToGCV(typ *sppb.Type, value *structpb.Value) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{Type: typ, Value: value}
}

func simpleGCVToNullable(value spanner.GenericColumnValue) (NullableValue, error) {
	switch value.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return decodeScalar[spanner.NullBool](value)
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return decodeScalar[spanner.NullInt64](value)
	case sppb.TypeCode_FLOAT32:
		return decodeScalar[spanner.NullFloat32](value)
	case sppb.TypeCode_FLOAT64:
		return decodeScalar[spanner.NullFloat64](value)
	case sppb.TypeCode_TIMESTAMP:
		return decodeScalar[spanner.NullTime](value)
	case sppb.TypeCode_DATE:
		return decodeScalar[spanner.NullDate](value)
	case sppb.TypeCode_STRING:
		return decodeScalar[spanner.NullString](value)
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return decodeScalar[NullBytes](value)
	case sppb.TypeCode_NUMERIC:
		return decodeScalar[spanner.NullNumeric](value)
	case sppb.TypeCode_JSON:
		return decodeScalar[spanner.NullJSON](value)
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return nil, fmt.Errorf("unknown type: %v", value.Type.String())
	}
}

func decodeScalar[T NullableValue](gcv spanner.GenericColumnValue) (T, error) {
	var v T
	err := gcv.Decode(&v)
	return v, err
}

func (fc *FormatConfig) formatSimpleColumn(value spanner.GenericColumnValue) (string, error) {
	nv, err := simpleGCVToNullable(value)
	if err != nil {
		return "", err
	}

	if nv.IsNull() {
		return fc.NullString, nil
	}

	return fc.FormatNullable(nv)
}

func isComplexType(elemCode sppb.TypeCode) bool {
	return sppb.TypeCode_STRUCT == elemCode || sppb.TypeCode_ARRAY == elemCode
}

var (
	_ FormatComplexFunc = FormatProtoAsCast
	_ FormatComplexFunc = FormatEnumAsCast
)

func FormatProtoAsCast(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
	if value.Type.GetCode() == sppb.TypeCode_PROTO {
		b, err := base64.StdEncoding.DecodeString(value.Value.GetStringValue())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`CAST(%v AS %v)`, internal.ToReadableBytesLiteral(b), value.Type.ProtoTypeFqn), nil
	}
	return "", ErrFallthrough
}

func FormatEnumAsCast(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
	if value.Type.GetCode() == sppb.TypeCode_ENUM {
		return fmt.Sprintf(`CAST(%q AS %v)`, value.Value.GetStringValue(), value.Type.ProtoTypeFqn), nil
	}
	return "", ErrFallthrough
}

type Formatter interface {
	FormatColumn(value spanner.GenericColumnValue, toplevel bool) (string, error)
}

type FormatConfig struct {
	NullString           string
	FormatArray          FormatArrayFunc
	FormatStruct         FormatStruct
	FormatComplexPlugins []FormatComplexFunc
	FormatNullable       FormatNullableFunc
}

type FormatStruct struct {
	FormatStructField FormatStructFieldFunc
	FormatStructParen FormatStructParenFunc
}

type FormatArrayFunc func(typ *sppb.Type, toplevel bool, elemStrings []string) string
type FormatStructParenFunc func(typ *sppb.Type, toplevel bool, fieldStrings []string) string
type FormatStructFieldFunc func(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error)
type FormatNullableFunc = func(value NullableValue) (string, error)

func (fc *FormatConfig) FormatColumn(value spanner.GenericColumnValue, toplevel bool) (string, error) {
	valType := value.Type
	switch valType.GetCode() {
	case sppb.TypeCode_ARRAY:
		// Note: This format is not intended to be parseable.
		if _, isNull := value.Value.Kind.(*structpb.Value_NullValue); isNull {
			return fc.NullString, nil
		}

		elemStrings, err := hiter.TryCollect(
			hiter.Divide(
				func(v *structpb.Value) (string, error) {
					return fc.FormatColumn(typeValueToGCV(valType.GetArrayElementType(), v), false)
				},
				slices.Values(value.Value.GetListValue().GetValues())))
		if err != nil {
			return "", err
		}

		return fc.FormatArray(value.Type, toplevel, elemStrings), nil
	case sppb.TypeCode_STRUCT:
		// Note: This format is not intended to be parseable.
		// There is no NULL struct.
		fieldStrings, err := hiter.TryCollect(xiter.Map2(
			func(field *sppb.StructType_Field, value *structpb.Value) (string, error) {
				return fc.FormatStruct.FormatStructField(fc, field, value)
			},
			hiter.Pairs(
				slices.Values(valType.GetStructType().GetFields()),
				slices.Values(value.Value.GetListValue().GetValues()))))
		if err != nil {
			return "", err
		}

		return fc.FormatStruct.FormatStructParen(value.Type, toplevel, fieldStrings), nil
	default:
		for _, f := range fc.FormatComplexPlugins {
			if s, err := f(fc, value, toplevel); !errors.Is(err, ErrFallthrough) {
				return s, err
			}
		}
		return fc.formatSimpleColumn(value)
	}
}

func (fc *FormatConfig) FormatRow(row *spanner.Row) ([]string, error) {
	gcvs := slices.Collect(hiter.RepeatFunc(lo.Empty[spanner.GenericColumnValue], row.Size()))
	if err := row.Columns(slices.Collect(internal.ToAny(internal.Pointers(gcvs)))...); err != nil {
		return nil, err
	}
	return hiter.TryCollect(hiter.Divide(func(gcv spanner.GenericColumnValue) (string, error) {
		return fc.FormatColumn(gcv, true)
	}, slices.Values(gcvs)))
}

func (fc *FormatConfig) FormatToplevelColumn(value spanner.GenericColumnValue) (string, error) {
	return fc.FormatColumn(value, true)
}
