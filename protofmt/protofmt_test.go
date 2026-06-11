package protofmt_test

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/protofmt"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	testProtoPackage = "example.music"
	singerInfoFQN    = testProtoPackage + ".SingerInfo"
	envelopeFQN      = testProtoPackage + ".Envelope"
	genreFQN         = testProtoPackage + ".Genre"
)

func TestFormatProtoTextValue_dynamicMessageRoundTrip(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	want := newSingerInfo(t, resolver, 1, "Alice", 1)
	payload := marshalProto(t, want)

	fc := descriptorAwareConfig(resolver)
	got, err := fc.FormatToplevelColumn(gcvctor.ProtoValue(singerInfoFQN, payload))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("formatted prototext has trailing newline: %q", got)
	}

	gotMessage := newEmptySingerInfo(t, resolver)
	if err := (prototext.UnmarshalOptions{Resolver: resolver}).Unmarshal([]byte(got), gotMessage); err != nil {
		t.Fatalf("unmarshal formatted prototext: %v\ntext:\n%s", err, got)
	}
	if !proto.Equal(want, gotMessage) {
		t.Fatalf("round-trip mismatch\nwant: %v\ngot:  %v", want, gotMessage)
	}
}

func TestFormatProtoTextValue_generatedMessageFromGlobalTypes(t *testing.T) {
	t.Parallel()

	want := durationpb.New(1500 * time.Millisecond)
	payload := marshalProto(t, want)

	fc := descriptorAwareConfig(protoregistry.GlobalTypes)
	got, err := fc.FormatToplevelColumn(gcvctor.ProtoValue("google.protobuf.Duration", payload))
	if err != nil {
		t.Fatal(err)
	}

	var gotMessage durationpb.Duration
	if err := prototext.Unmarshal([]byte(got), &gotMessage); err != nil {
		t.Fatalf("unmarshal formatted prototext: %v\ntext:\n%s", err, got)
	}
	if !proto.Equal(want, &gotMessage) {
		t.Fatalf("round-trip mismatch\nwant: %v\ngot:  %v", want, &gotMessage)
	}
}

func TestFormatProtoTextValue_usesMarshalResolverForAnyExpansion(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	singer := newSingerInfo(t, resolver, 1, "Alice", 1)
	envelope := newEnvelopeWithAny(t, resolver, singer)
	payload := marshalProto(t, envelope)

	fc := descriptorAwareConfig(resolver)
	got, err := fc.FormatToplevelColumn(gcvctor.ProtoValue(envelopeFQN, payload))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "type.googleapis.com/"+singerInfoFQN) {
		t.Fatalf("formatted Any was not expanded with the custom resolver:\n%s", got)
	}
	if !strings.Contains(got, "singer_id") || !strings.Contains(got, "Alice") {
		t.Fatalf("expanded Any did not include SingerInfo fields:\n%s", got)
	}
}

func TestFormatEnumNameValue_dynamicEnum(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	fc := descriptorAwareConfig(resolver)

	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{
			name: "known value",
			gcv:  gcvctor.EnumValue(genreFQN, 1),
			want: "POP",
		},
		{
			name: "unknown value",
			gcv:  gcvctor.EnumValue(genreFQN, 99),
			want: "99",
		},
		{
			name: "out of int32 range",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: genreFQN},
				Value: structpb.NewStringValue("2147483648"),
			},
			want: "2147483648",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := fc.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatPlugins_fallthroughCases(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	tests := []struct {
		name   string
		plugin spanvalue.FormatComplexFunc
		gcv    spanner.GenericColumnValue
	}{
		{
			name:   "proto nil resolver",
			plugin: protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{}),
			gcv:    gcvctor.ProtoValue(singerInfoFQN, nil),
		},
		{
			name:   "proto empty fqn",
			plugin: protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver}),
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO},
				Value: structpb.NewStringValue(""),
			},
		},
		{
			name:   "proto missing type",
			plugin: protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver}),
			gcv:    gcvctor.ProtoValue("example.music.Missing", nil),
		},
		{
			name:   "proto missing type from GlobalTypes",
			plugin: protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: protoregistry.GlobalTypes}),
			gcv:    gcvctor.ProtoValue("example.music.Missing", nil),
		},
		{
			name: "proto nil message type from resolver",
			plugin: protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{
				Resolver: fakeResolver{
					findMessageByName: func(protoreflect.FullName) (protoreflect.MessageType, error) {
						return nil, nil
					},
				},
			}),
			gcv: gcvctor.ProtoValue(singerInfoFQN, nil),
		},
		{
			name:   "enum nil resolver",
			plugin: protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{}),
			gcv:    gcvctor.EnumValue(genreFQN, 1),
		},
		{
			name:   "enum empty fqn",
			plugin: protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver}),
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM},
				Value: structpb.NewStringValue("1"),
			},
		},
		{
			name:   "enum missing type",
			plugin: protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver}),
			gcv:    gcvctor.EnumValue("example.music.Missing", 1),
		},
		{
			name:   "enum missing type from GlobalTypes",
			plugin: protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: protoregistry.GlobalTypes}),
			gcv:    gcvctor.EnumValue("example.music.Missing", 1),
		},
		{
			name: "enum nil enum type from resolver",
			plugin: protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{
				Resolver: fakeResolver{
					findEnumByName: func(protoreflect.FullName) (protoreflect.EnumType, error) {
						return nil, nil
					},
				},
			}),
			gcv: gcvctor.EnumValue(genreFQN, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.plugin(spanvalue.SpannerCLICompatibleFormatConfig(), tt.gcv, true)
			if !errors.Is(err, spanvalue.ErrFallthrough) {
				t.Fatalf("error = %v, want ErrFallthrough", err)
			}
		})
	}
}

func TestFormatPlugins_onUnresolvedInvoked(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	nilTypeProtoResolver := fakeResolver{
		findMessageByName: func(protoreflect.FullName) (protoreflect.MessageType, error) {
			return nil, nil
		},
	}
	nilTypeEnumResolver := fakeResolver{
		findEnumByName: func(protoreflect.FullName) (protoreflect.EnumType, error) {
			return nil, nil
		},
	}

	tests := []struct {
		name     string
		plugin   func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc
		gcv      spanner.GenericColumnValue
		wantFQN  string
		wantCode sppb.TypeCode
	}{
		{
			name: "proto not found",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv:      gcvctor.ProtoValue("example.music.Missing", nil),
			wantFQN:  "example.music.Missing",
			wantCode: sppb.TypeCode_PROTO,
		},
		{
			name: "proto nil message type from resolver",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: nilTypeProtoResolver, OnUnresolved: onUnresolved})
			},
			gcv:      gcvctor.ProtoValue(singerInfoFQN, nil),
			wantFQN:  singerInfoFQN,
			wantCode: sppb.TypeCode_PROTO,
		},
		{
			name: "enum not found",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv:      gcvctor.EnumValue("example.music.Missing", 1),
			wantFQN:  "example.music.Missing",
			wantCode: sppb.TypeCode_ENUM,
		},
		{
			name: "enum nil enum type from resolver",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: nilTypeEnumResolver, OnUnresolved: onUnresolved})
			},
			gcv:      gcvctor.EnumValue(genreFQN, 1),
			wantFQN:  genreFQN,
			wantCode: sppb.TypeCode_ENUM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls int
			plugin := tt.plugin(func(typeFQN string, code sppb.TypeCode) error {
				calls++
				if typeFQN != tt.wantFQN {
					t.Errorf("typeFQN = %q, want %q", typeFQN, tt.wantFQN)
				}
				if code != tt.wantCode {
					t.Errorf("code = %v, want %v", code, tt.wantCode)
				}
				return nil
			})

			_, err := plugin(spanvalue.SpannerCLICompatibleFormatConfig(), tt.gcv, true)
			if !errors.Is(err, spanvalue.ErrFallthrough) {
				t.Fatalf("error = %v, want ErrFallthrough when handler returns nil", err)
			}
			if calls != 1 {
				t.Fatalf("OnUnresolved calls = %d, want 1", calls)
			}
		})
	}
}

func TestFormatPlugins_onUnresolvedNotInvoked(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	singerPayload := marshalProto(t, newSingerInfo(t, resolver, 1, "Alice", 1))

	tests := []struct {
		name            string
		plugin          func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc
		gcv             spanner.GenericColumnValue
		wantFallthrough bool
	}{
		{
			name: "proto nil resolver",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{OnUnresolved: onUnresolved})
			},
			gcv:             gcvctor.ProtoValue("example.music.Missing", nil),
			wantFallthrough: true,
		},
		{
			name: "proto typed null",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: gcvctor.NullOf(&sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: "example.music.Missing"}),
		},
		{
			name: "proto empty fqn",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO},
				Value: structpb.NewStringValue(""),
			},
			wantFallthrough: true,
		},
		{
			name: "proto resolution succeeds",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: gcvctor.ProtoValue(singerInfoFQN, singerPayload),
		},
		{
			name: "non-proto column",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv:             gcvctor.StringValue("plain"),
			wantFallthrough: true,
		},
		{
			name: "enum nil resolver",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{OnUnresolved: onUnresolved})
			},
			gcv:             gcvctor.EnumValue("example.music.Missing", 1),
			wantFallthrough: true,
		},
		{
			name: "enum typed null",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: gcvctor.NullOf(&sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: "example.music.Missing"}),
		},
		{
			name: "enum empty fqn",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM},
				Value: structpb.NewStringValue("1"),
			},
			wantFallthrough: true,
		},
		{
			name: "enum resolution succeeds",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv: gcvctor.EnumValue(genreFQN, 1),
		},
		{
			name: "non-enum column",
			plugin: func(onUnresolved func(string, sppb.TypeCode) error) spanvalue.FormatComplexFunc {
				return protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved})
			},
			gcv:             gcvctor.StringValue("plain"),
			wantFallthrough: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls int
			plugin := tt.plugin(func(string, sppb.TypeCode) error {
				calls++
				return errors.New("OnUnresolved must not be invoked")
			})

			_, err := plugin(spanvalue.SpannerCLICompatibleFormatConfig(), tt.gcv, true)
			if tt.wantFallthrough {
				if !errors.Is(err, spanvalue.ErrFallthrough) {
					t.Fatalf("error = %v, want ErrFallthrough", err)
				}
			} else if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if calls != 0 {
				t.Fatalf("OnUnresolved calls = %d, want 0", calls)
			}
		})
	}
}

func TestFormatPlugins_onUnresolvedErrorPropagates(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	sentinel := errors.New("strict: unresolved type")
	onUnresolved := func(string, sppb.TypeCode) error { return sentinel }

	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatComplexPlugins = append(
		[]spanvalue.FormatComplexFunc{
			protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved}),
			protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved}),
		},
		fc.FormatComplexPlugins...,
	)

	tests := []spanner.GenericColumnValue{
		gcvctor.ProtoValue("example.music.Missing", nil),
		gcvctor.EnumValue("example.music.Missing", 1),
	}
	for _, gcv := range tests {
		_, err := fc.FormatToplevelColumn(gcv)
		if !errors.Is(err, sentinel) {
			t.Fatalf("error = %v, want %v", err, sentinel)
		}
	}
}

func TestFormatPlugins_onUnresolvedNilResultKeepsWireOutput(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	lenient := descriptorAwareConfig(resolver)

	observing := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	onUnresolved := func(string, sppb.TypeCode) error { return nil }
	observing.FormatComplexPlugins = append(
		[]spanvalue.FormatComplexFunc{
			protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: onUnresolved}),
			protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: onUnresolved}),
		},
		observing.FormatComplexPlugins...,
	)

	tests := []spanner.GenericColumnValue{
		gcvctor.ProtoValue("example.music.Missing", []byte{0x08, 0x01}),
		gcvctor.EnumValue("example.music.Missing", 1),
	}
	for _, gcv := range tests {
		want, err := lenient.FormatToplevelColumn(gcv)
		if err != nil {
			t.Fatal(err)
		}
		got, err := observing.FormatToplevelColumn(gcv)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %q, want wire-form fallthrough %q", got, want)
		}
	}
}

func TestFormatPlugins_typedNullUsesFormatter(t *testing.T) {
	t.Parallel()

	fc := descriptorAwareConfig(nil)
	fc.NullString = "<NULL>"

	tests := []spanner.GenericColumnValue{
		gcvctor.NullOf(&sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: singerInfoFQN}),
		gcvctor.NullOf(&sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: genreFQN}),
	}
	for _, gcv := range tests {
		got, err := fc.FormatToplevelColumn(gcv)
		if err != nil {
			t.Fatal(err)
		}
		if got != "<NULL>" {
			t.Fatalf("got %q, want <NULL>", got)
		}
	}
}

func TestFormatPlugins_returnRealErrors(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
	}{
		{
			name: "proto malformed wire kind",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: singerInfoFQN},
				Value: structpb.NewNumberValue(1),
			},
		},
		{
			name: "proto base64 decode",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: singerInfoFQN},
				Value: structpb.NewStringValue("not-base64!"),
			},
		},
		{
			name: "proto unmarshal",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: singerInfoFQN},
				Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString([]byte{0xff})),
			},
		},
		{
			name: "enum malformed wire kind",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: genreFQN},
				Value: structpb.NewNumberValue(1),
			},
		},
		{
			name: "enum non-numeric",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: genreFQN},
				Value: structpb.NewStringValue("POP"),
			},
		},
	}

	fc := descriptorAwareConfig(resolver)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := fc.FormatToplevelColumn(tt.gcv)
			if err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(err, spanvalue.ErrFallthrough) {
				t.Fatalf("error = %v, want real error", err)
			}
		})
	}
}

func TestFormatPlugins_nestedDispatch(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	payload := marshalProto(t, newSingerInfo(t, resolver, 1, "Alice", 1))
	protoGCV := gcvctor.ProtoValue(singerInfoFQN, payload)
	enumGCV := gcvctor.EnumValue(genreFQN, 1)

	fc := descriptorAwareConfig(resolver)

	arrayGCV, err := gcvctor.ArrayValue(protoGCV)
	if err != nil {
		t.Fatal(err)
	}
	gotArray, err := fc.FormatToplevelColumn(arrayGCV)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotArray, "singer_id") || !strings.Contains(gotArray, "Alice") {
		t.Fatalf("array output did not use proto formatter: %q", gotArray)
	}

	structGCV, err := gcvctor.StructValueOf([]string{"genre", "info"}, []spanner.GenericColumnValue{enumGCV, protoGCV})
	if err != nil {
		t.Fatal(err)
	}
	gotStruct, err := fc.FormatToplevelColumn(structGCV)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotStruct, "POP") || !strings.Contains(gotStruct, "singer_id") {
		t.Fatalf("struct output did not use proto/enum formatters: %q", gotStruct)
	}
}

func TestProtoEnumResolverFromFileDescriptorSet_nilIsEmptyResolver(t *testing.T) {
	t.Parallel()

	resolver, err := protofmt.ProtoEnumResolverFromFileDescriptorSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.FindMessageByName("example.Missing")
	if !isExactNotFound(err) {
		t.Fatalf("error = %v, want exact protoregistry.NotFound", err)
	}
}

func TestComposeProtoEnumResolvers_orderedLookup(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	want, err := resolver.FindMessageByName(singerInfoFQN)
	if err != nil {
		t.Fatal(err)
	}

	var calls []string
	first := fakeResolver{
		findMessageByName: func(protoreflect.FullName) (protoreflect.MessageType, error) {
			calls = append(calls, "first")
			return nil, protoregistry.NotFound
		},
	}
	second := fakeResolver{
		findMessageByName: func(protoreflect.FullName) (protoreflect.MessageType, error) {
			calls = append(calls, "second")
			return want, nil
		},
	}

	got, err := protofmt.ComposeProtoEnumResolvers(first, nil, second).FindMessageByName(singerInfoFQN)
	if err != nil {
		t.Fatal(err)
	}
	if got.Descriptor().FullName() != want.Descriptor().FullName() {
		t.Fatalf("got %v, want %v", got.Descriptor().FullName(), want.Descriptor().FullName())
	}
	if strings.Join(calls, ",") != "first,second" {
		t.Fatalf("calls = %v, want [first second]", calls)
	}
}

func TestComposeProtoEnumResolvers_singleActiveResolver(t *testing.T) {
	t.Parallel()

	resolver := testResolver(t)
	got := protofmt.ComposeProtoEnumResolvers(nil, resolver, nil)
	if got != resolver {
		t.Fatalf("got %T, want original resolver %T", got, resolver)
	}
}

func TestComposeProtoEnumResolvers_emptyResolverReturnsNotFound(t *testing.T) {
	t.Parallel()

	resolver := protofmt.ComposeProtoEnumResolvers(nil)
	_, err := resolver.FindMessageByName("example.Missing")
	if !isExactNotFound(err) {
		t.Fatalf("error = %v, want exact protoregistry.NotFound", err)
	}
}

func TestComposeProtoEnumResolvers_errorSemantics(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	wrappedNotFound := fmt.Errorf("wrapped: %w", protoregistry.NotFound)

	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "first non-notfound error", err: boom, want: boom},
		{name: "wrapped notfound is not skipped", err: wrappedNotFound, want: wrappedNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver := protofmt.ComposeProtoEnumResolvers(
				fakeResolver{
					findMessageByName: func(protoreflect.FullName) (protoreflect.MessageType, error) {
						return nil, tt.err
					},
				},
				testResolver(t),
			)
			_, err := resolver.FindMessageByName(singerInfoFQN)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestComposeProtoEnumResolvers_allMethodsUseNotFoundFallback(t *testing.T) {
	t.Parallel()

	firstCalls := 0
	secondCalls := 0
	first := countingResolver{err: protoregistry.NotFound, calls: &firstCalls}
	second := countingResolver{err: nil, calls: &secondCalls}
	resolver := protofmt.ComposeProtoEnumResolvers(first, second)

	if _, err := resolver.FindMessageByName("example.Message"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.FindMessageByURL("type.googleapis.com/example.Message"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.FindExtensionByName("example.ext"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.FindExtensionByNumber("example.Message", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.FindEnumByName("example.Enum"); err != nil {
		t.Fatal(err)
	}
	if firstCalls != 5 || secondCalls != 5 {
		t.Fatalf("calls = first:%d second:%d, want 5 and 5", firstCalls, secondCalls)
	}

	_, err := protofmt.ComposeProtoEnumResolvers(first).FindEnumByName("example.Missing")
	if !isExactNotFound(err) {
		t.Fatalf("error = %v, want exact protoregistry.NotFound", err)
	}
}

func descriptorAwareConfig(resolver protofmt.ProtoEnumResolver) *spanvalue.FormatConfig {
	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatComplexPlugins = append(
		[]spanvalue.FormatComplexFunc{
			protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{
				Resolver: resolver,
				Marshal:  prototext.MarshalOptions{Multiline: true},
			}),
			protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver}),
		},
		fc.FormatComplexPlugins...,
	)
	return fc
}

func testResolver(t *testing.T) protofmt.ProtoEnumResolver {
	t.Helper()

	resolver, err := protofmt.ProtoEnumResolverFromFileDescriptorSet(testFileDescriptorSet())
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func newSingerInfo(t *testing.T, resolver protofmt.ProtoEnumResolver, id int64, name string, genre protoreflect.EnumNumber) proto.Message {
	t.Helper()

	message := newEmptySingerInfo(t, resolver).ProtoReflect()
	fields := message.Descriptor().Fields()
	message.Set(fields.ByName("singer_id"), protoreflect.ValueOfInt64(id))
	message.Set(fields.ByName("name"), protoreflect.ValueOfString(name))
	message.Set(fields.ByName("genre"), protoreflect.ValueOfEnum(genre))
	return message.Interface()
}

func newEmptySingerInfo(t *testing.T, resolver protofmt.ProtoEnumResolver) proto.Message {
	t.Helper()

	messageType, err := resolver.FindMessageByName(singerInfoFQN)
	if err != nil {
		t.Fatal(err)
	}
	return messageType.New().Interface()
}

func newEnvelopeWithAny(t *testing.T, resolver protofmt.ProtoEnumResolver, message proto.Message) proto.Message {
	t.Helper()

	envelopeType, err := resolver.FindMessageByName(envelopeFQN)
	if err != nil {
		t.Fatal(err)
	}
	anyType, err := resolver.FindMessageByName("google.protobuf.Any")
	if err != nil {
		t.Fatal(err)
	}

	anyMessage := anyType.New()
	anyFields := anyMessage.Descriptor().Fields()
	anyMessage.Set(anyFields.ByName("type_url"), protoreflect.ValueOfString("type.googleapis.com/"+string(message.ProtoReflect().Descriptor().FullName())))
	anyMessage.Set(anyFields.ByName("value"), protoreflect.ValueOfBytes(marshalProto(t, message)))

	envelope := envelopeType.New()
	envelopeFields := envelope.Descriptor().Fields()
	envelope.Set(envelopeFields.ByName("info"), protoreflect.ValueOfMessage(anyMessage))
	return envelope.Interface()
}

func marshalProto(t *testing.T, m proto.Message) []byte {
	t.Helper()

	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func isExactNotFound(err error) bool {
	return err == protoregistry.NotFound //nolint:errorlint
}

func testFileDescriptorSet() *descriptorpb.FileDescriptorSet {
	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(anypb.File_google_protobuf_any_proto),
			{
				Name:    proto.String("music.proto"),
				Package: proto.String(testProtoPackage),
				Syntax:  proto.String("proto3"),
				Dependency: []string{
					"google/protobuf/any.proto",
				},
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: proto.String("Genre"),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{Name: proto.String("GENRE_UNSPECIFIED"), Number: proto.Int32(0)},
							{Name: proto.String("POP"), Number: proto.Int32(1)},
							{Name: proto.String("ROCK"), Number: proto.Int32(2)},
						},
					},
				},
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("SingerInfo"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("singer_id"),
								JsonName: proto.String("singerId"),
								Number:   proto.Int32(1),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
							},
							{
								Name:     proto.String("name"),
								JsonName: proto.String("name"),
								Number:   proto.Int32(2),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
							},
							{
								Name:     proto.String("genre"),
								JsonName: proto.String("genre"),
								Number:   proto.Int32(3),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
								TypeName: proto.String("." + genreFQN),
							},
						},
					},
					{
						Name: proto.String("Envelope"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("info"),
								JsonName: proto.String("info"),
								Number:   proto.Int32(1),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
								TypeName: proto.String(".google.protobuf.Any"),
							},
						},
					},
				},
			},
		},
	}
}

type fakeResolver struct {
	findMessageByName     func(protoreflect.FullName) (protoreflect.MessageType, error)
	findMessageByURL      func(string) (protoreflect.MessageType, error)
	findExtensionByName   func(protoreflect.FullName) (protoreflect.ExtensionType, error)
	findExtensionByNumber func(protoreflect.FullName, protoreflect.FieldNumber) (protoreflect.ExtensionType, error)
	findEnumByName        func(protoreflect.FullName) (protoreflect.EnumType, error)
}

func (r fakeResolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if r.findMessageByName != nil {
		return r.findMessageByName(name)
	}
	return nil, protoregistry.NotFound
}

func (r fakeResolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	if r.findMessageByURL != nil {
		return r.findMessageByURL(url)
	}
	return nil, protoregistry.NotFound
}

func (r fakeResolver) FindExtensionByName(name protoreflect.FullName) (protoreflect.ExtensionType, error) {
	if r.findExtensionByName != nil {
		return r.findExtensionByName(name)
	}
	return nil, protoregistry.NotFound
}

func (r fakeResolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	if r.findExtensionByNumber != nil {
		return r.findExtensionByNumber(message, field)
	}
	return nil, protoregistry.NotFound
}

func (r fakeResolver) FindEnumByName(name protoreflect.FullName) (protoreflect.EnumType, error) {
	if r.findEnumByName != nil {
		return r.findEnumByName(name)
	}
	return nil, protoregistry.NotFound
}

type countingResolver struct {
	err   error
	calls *int
}

func (r countingResolver) inc() {
	*r.calls = *r.calls + 1
}

func (r countingResolver) FindMessageByName(protoreflect.FullName) (protoreflect.MessageType, error) {
	r.inc()
	return nil, r.err
}

func (r countingResolver) FindMessageByURL(string) (protoreflect.MessageType, error) {
	r.inc()
	return nil, r.err
}

func (r countingResolver) FindExtensionByName(protoreflect.FullName) (protoreflect.ExtensionType, error) {
	r.inc()
	return nil, r.err
}

func (r countingResolver) FindExtensionByNumber(protoreflect.FullName, protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	r.inc()
	return nil, r.err
}

func (r countingResolver) FindEnumByName(protoreflect.FullName) (protoreflect.EnumType, error) {
	r.inc()
	return nil, r.err
}
