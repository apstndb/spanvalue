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

// WithEquivalentType returns gcv retyped to typ when [github.com/apstndb/spantype.EquivalentTypes]
// reports the source and destination types are Spanner-equivalent.
func WithEquivalentType(typ *sppb.Type, gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if typ == nil {
		return spanner.GenericColumnValue{}, ErrNilDestinationType
	}
	if gcv.Type == nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("%w: source type is nil", ErrTypeMismatch)
	}
	if !spantype.EquivalentTypes(gcv.Type, typ) {
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
	if gcv.Type == nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("%w: source type is nil", ErrTypeMismatch)
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
