package protofmt_test

import (
	"fmt"

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
