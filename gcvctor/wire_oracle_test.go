package gcvctor_test

import (
	"encoding/base64"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/gcvctor"
)

func TestEnumProtoAndBytesBasedValueOracle(t *testing.T) {
	t.Parallel()

	enumFQN := "com.example.Color"
	protoFQN := "com.example.User"
	payload := []byte{0x01, 0xff}

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{
			name: "enum",
			got:  gcvctor.EnumValue(enumFQN, -7),
			want: spanner.GenericColumnValue{
				Type:  typector.FQNToEnumType(enumFQN),
				Value: structpb.NewStringValue("-7"),
			},
		},
		{
			name: "proto",
			got:  gcvctor.ProtoValue(protoFQN, payload),
			want: spanner.GenericColumnValue{
				Type:  typector.FQNToProtoType(protoFQN),
				Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(payload)),
			},
		},
		{
			name: "bytes based",
			got:  gcvctor.BytesBasedValueOf(typector.FQNToProtoType(protoFQN), payload),
			want: spanner.GenericColumnValue{
				Type:  typector.FQNToProtoType(protoFQN),
				Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(payload)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, tt.got, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
