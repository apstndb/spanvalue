package protofmt_test

import (
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/protofmt"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

func Example_spannerCLICompatibleFormatConfigWithProto() {
	var fds *descriptorpb.FileDescriptorSet
	// Load fds in the application, for example from a descriptor set produced
	// with imports included. A nil fds is valid: it creates an empty resolver,
	// so the plugins fall through to the preset's descriptor-free formatting.
	dynamicResolver, err := protofmt.ProtoEnumResolverFromFileDescriptorSet(fds)
	if err != nil {
		panic(err)
	}

	resolver := protofmt.ComposeProtoEnumResolvers(
		dynamicResolver,
		protoregistry.GlobalTypes, // for generated protobuf packages linked into the binary
	)

	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatComplexPlugins = append(
		[]spanvalue.FormatComplexFunc{
			protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver}),
			protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver}),
		},
		fc.FormatComplexPlugins...,
	)

	out, err := fc.FormatToplevelColumn(gcvctor.EnumValue(
		"google.protobuf.FieldDescriptorProto.Type",
		int64(descriptorpb.FieldDescriptorProto_TYPE_STRING.Number()),
	))
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output:
	// TYPE_STRING
}

// Strict mode: when a resolver is configured but a non-NULL PROTO or ENUM
// value cannot be resolved, a one-line OnUnresolved handler turns the silent
// wire-form fallthrough into an error surfaced to the formatter caller.
func ExampleProtoTextValueOptions_onUnresolved() {
	// An empty resolver stands in for a misconfigured descriptor set (for
	// example a missing import or a type FQN typo).
	resolver, err := protofmt.ProtoEnumResolverFromFileDescriptorSet(nil)
	if err != nil {
		panic(err)
	}

	strict := func(typeFQN string, code sppb.TypeCode) error {
		return fmt.Errorf("protofmt: unresolved %v type %q", code, typeFQN)
	}

	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatComplexPlugins = append(
		[]spanvalue.FormatComplexFunc{
			protofmt.FormatProtoTextValue(protofmt.ProtoTextValueOptions{Resolver: resolver, OnUnresolved: strict}),
			protofmt.FormatEnumNameValue(protofmt.EnumNameValueOptions{Resolver: resolver, OnUnresolved: strict}),
		},
		fc.FormatComplexPlugins...,
	)

	_, err = fc.FormatToplevelColumn(gcvctor.ProtoValue("example.music.SingerInfo", nil))
	fmt.Println(err)
	// Output:
	// protofmt: unresolved PROTO type "example.music.SingerInfo"
}
