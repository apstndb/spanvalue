package gcvctor

import (
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
	"google.golang.org/protobuf/proto"
)

// ErrNilDestinationType is returned by [WithEquivalentType] and [WithExactType]
// when typ is nil.
var ErrNilDestinationType = errors.New("gcvctor: nil destination type")

// WithType returns a copy of gcv with Type replaced by typ and Value unchanged.
// A nil typ is normalized to TYPE_CODE_UNSPECIFIED, matching [NullOf].
// For validation, use [WithEquivalentType] or [WithExactType].
func WithType(typ *sppb.Type, gcv spanner.GenericColumnValue) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  normalizeNilType(typ),
		Value: gcv.Value,
	}
}

// WithEquivalentType returns gcv retyped to typ when the source and destination
// types are Spanner-equivalent: scalar types require proto.Equal metadata;
// ARRAY types require equivalent element types; STRUCT types require the same
// number of fields with pairwise equivalent field types (field names are not
// compared). This mirrors identity CAST and ARRAY cast paths in semantic layers.
func WithEquivalentType(typ *sppb.Type, gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if typ == nil {
		return spanner.GenericColumnValue{}, ErrNilDestinationType
	}
	if !equivalentTypes(gcv.Type, typ) {
		return spanner.GenericColumnValue{}, fmt.Errorf(
			"%w: %v is not equivalent to %v",
			ErrTypeMismatch,
			spantype.FormatTypeMoreVerbose(gcv.Type),
			spantype.FormatTypeMoreVerbose(typ),
		)
	}
	return WithType(typ, gcv), nil
}

// WithExactType returns gcv retyped to typ when proto.Equal(gcv.Type, typ).
// Use this when expected-type coercion requires identical type metadata, not
// merely Spanner equivalence.
func WithExactType(typ *sppb.Type, gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if typ == nil {
		return spanner.GenericColumnValue{}, ErrNilDestinationType
	}
	if !proto.Equal(gcv.Type, typ) {
		return spanner.GenericColumnValue{}, fmt.Errorf(
			"%w: destination type metadata differs from %v",
			ErrTypeMismatch,
			spantype.FormatTypeMoreVerbose(gcv.Type),
		)
	}
	return WithType(typ, gcv), nil
}

func equivalentTypes(a, b *sppb.Type) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.GetCode() != b.GetCode() {
		return false
	}
	switch a.GetCode() {
	case sppb.TypeCode_ARRAY:
		return equivalentTypes(a.GetArrayElementType(), b.GetArrayElementType())
	case sppb.TypeCode_STRUCT:
		aStruct := a.GetStructType()
		bStruct := b.GetStructType()
		if aStruct == nil || bStruct == nil {
			return aStruct == nil && bStruct == nil
		}
		aFields := aStruct.GetFields()
		bFields := bStruct.GetFields()
		if len(aFields) != len(bFields) {
			return false
		}
		for i := range aFields {
			if aFields[i] == nil || bFields[i] == nil {
				if aFields[i] != bFields[i] {
					return false
				}
				continue
			}
			if !equivalentTypes(aFields[i].GetType(), bFields[i].GetType()) {
				return false
			}
		}
		return true
	default:
		return proto.Equal(a, b)
	}
}
