package protofmt

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	minEnumNumber = int64(-1 << 31)
	maxEnumNumber = int64(1<<31 - 1)
)

// ProtoEnumResolver resolves protobuf message, extension, and enum types for
// descriptor-aware Spanner PROTO and ENUM display.
type ProtoEnumResolver interface {
	protoregistry.MessageTypeResolver
	protoregistry.ExtensionTypeResolver
	EnumResolver
}

// EnumResolver resolves protobuf enum types for descriptor-aware Spanner ENUM
// display.
type EnumResolver interface {
	FindEnumByName(protoreflect.FullName) (protoreflect.EnumType, error)
}

var (
	_ ProtoEnumResolver = (*dynamicpb.Types)(nil)
	_ ProtoEnumResolver = (*protoregistry.Types)(nil)

	_ spanvalue.FormatComplexFunc = FormatProtoTextValue(ProtoTextValueOptions{})
	_ spanvalue.FormatComplexFunc = FormatEnumNameValue(EnumNameValueOptions{})
)

// ProtoTextValueOptions configures [FormatProtoTextValue].
//
// Resolver is authoritative: [FormatProtoTextValue] copies Unmarshal and
// Marshal, then overwrites both local Resolver fields from Resolver without
// mutating caller-owned options.
type ProtoTextValueOptions struct {
	Resolver  ProtoEnumResolver
	Unmarshal proto.UnmarshalOptions
	Marshal   prototext.MarshalOptions

	// OnUnresolved, when non-nil, is invoked before falling through when
	// Resolver is non-nil but cannot resolve the message type of a non-NULL
	// PROTO value with a non-empty type FQN (lookup returns exact
	// [protoregistry.NotFound] or a nil type). If OnUnresolved returns a
	// non-nil error, the plugin returns that error to the formatter caller;
	// if it returns nil, the plugin falls through to wire-form output as
	// usual. A nil OnUnresolved keeps the default lenient behavior.
	//
	// OnUnresolved is never invoked for nil resolvers, non-PROTO values,
	// typed NULL values, empty type FQNs, or successful resolution. Returning
	// an error from OnUnresolved is the strict-mode recipe; see the example.
	OnUnresolved func(typeFQN string, code sppb.TypeCode) error
}

// EnumNameValueOptions configures [FormatEnumNameValue].
type EnumNameValueOptions struct {
	Resolver EnumResolver

	// OnUnresolved, when non-nil, is invoked before falling through when
	// Resolver is non-nil but cannot resolve the enum type of a non-NULL
	// ENUM value with a non-empty type FQN (lookup returns exact
	// [protoregistry.NotFound] or a nil type). If OnUnresolved returns a
	// non-nil error, the plugin returns that error to the formatter caller;
	// if it returns nil, the plugin falls through to wire-form output as
	// usual. A nil OnUnresolved keeps the default lenient behavior.
	//
	// OnUnresolved is never invoked for nil resolvers, non-ENUM values,
	// typed NULL values, empty type FQNs, or successful resolution. Returning
	// an error from OnUnresolved is the strict-mode recipe; see
	// [ProtoTextValueOptions.OnUnresolved] for the parallel PROTO option.
	OnUnresolved func(typeFQN string, code sppb.TypeCode) error
}

// FormatProtoTextValue returns a spanvalue plugin that formats Spanner PROTO
// values as protobuf text format when opts.Resolver can resolve the message
// type.
//
// The plugin returns [spanvalue.ErrFallthrough] for non-PROTO values, nil or
// missing resolvers, empty type names, missing message types, and typed NULL
// PROTO values (without consulting the resolver; the chain's built-in
// handling renders NULL via [spanvalue.Formatter.GetNullString]).
// Malformed non-NULL wire payloads, base64 decode failures, unmarshal failures,
// and marshal failures are returned as real errors.
//
// [ProtoTextValueOptions.OnUnresolved] optionally observes (and can turn into
// errors) the missing-message-type fallthrough when a non-nil resolver is
// configured.
//
// Protobuf text output is display-oriented and intentionally not stable. Tests
// and callers must not depend on byte-for-byte stable output.
func FormatProtoTextValue(opts ProtoTextValueOptions) spanvalue.FormatComplexFunc {
	resolver := opts.Resolver
	if isNilResolver(resolver) {
		// Nothing can be decoded without a resolver; defer everything.
		// NULL PROTO falls through too — the built-in scalar handling
		// renders it via GetNullString on every preset.
		return fallthroughPlugin
	}

	unmarshal := opts.Unmarshal
	unmarshal.Resolver = resolver
	marshal := opts.Marshal
	marshal.Resolver = resolver
	onUnresolved := opts.OnUnresolved

	return spanvalue.PluginForTypeCode(sppb.TypeCode_PROTO, spanvalue.PluginSkippingNull(func(formatter spanvalue.Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		typeName := protoreflect.FullName(value.Type.GetProtoTypeFqn())
		if typeName == "" {
			return "", spanvalue.ErrFallthrough
		}
		messageType, err := resolver.FindMessageByName(typeName)
		if isExactNotFound(err) {
			return "", unresolvedFallthrough(onUnresolved, typeName, sppb.TypeCode_PROTO)
		}
		if err != nil {
			return "", err
		}
		if messageType == nil {
			return "", unresolvedFallthrough(onUnresolved, typeName, sppb.TypeCode_PROTO)
		}

		wire, err := stringWire(value, sppb.TypeCode_PROTO)
		if err != nil {
			return "", err
		}
		payload, err := base64.StdEncoding.DecodeString(wire)
		if err != nil {
			return "", err
		}

		message := messageType.New()
		if err := unmarshal.Unmarshal(payload, message.Interface()); err != nil {
			return "", err
		}

		out, err := marshal.Marshal(message.Interface())
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(string(out), "\n"), nil
	}))
}

// FormatEnumNameValue returns a spanvalue plugin that formats Spanner ENUM
// values as enum value names when opts.Resolver can resolve the enum type and
// value number.
//
// The plugin returns [spanvalue.ErrFallthrough] for non-ENUM values, nil or
// missing resolvers, empty type names, missing enum types, and typed NULL
// ENUM values (without consulting the resolver; the chain's built-in
// handling renders NULL via [spanvalue.Formatter.GetNullString]). Known
// enum types with unknown or out-of-range numeric values return the original
// numeric string.
//
// [EnumNameValueOptions.OnUnresolved] optionally observes (and can turn into
// errors) the missing-enum-type fallthrough when a non-nil resolver is
// configured.
func FormatEnumNameValue(opts EnumNameValueOptions) spanvalue.FormatComplexFunc {
	resolver := opts.Resolver
	if isNilResolver(resolver) {
		// See FormatProtoTextValue: defer everything, NULL included.
		return fallthroughPlugin
	}

	onUnresolved := opts.OnUnresolved
	return spanvalue.PluginForTypeCode(sppb.TypeCode_ENUM, spanvalue.PluginSkippingNull(func(formatter spanvalue.Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		typeName := protoreflect.FullName(value.Type.GetProtoTypeFqn())
		if typeName == "" {
			return "", spanvalue.ErrFallthrough
		}
		enumType, err := resolver.FindEnumByName(typeName)
		if isExactNotFound(err) {
			return "", unresolvedFallthrough(onUnresolved, typeName, sppb.TypeCode_ENUM)
		}
		if err != nil {
			return "", err
		}
		if enumType == nil {
			return "", unresolvedFallthrough(onUnresolved, typeName, sppb.TypeCode_ENUM)
		}

		wire, err := stringWire(value, sppb.TypeCode_ENUM)
		if err != nil {
			return "", err
		}
		n, err := strconv.ParseInt(wire, 10, 64)
		if err != nil {
			return "", err
		}
		if n < minEnumNumber || n > maxEnumNumber {
			return wire, nil
		}

		valueDesc := enumType.Descriptor().Values().ByNumber(protoreflect.EnumNumber(n))
		if valueDesc == nil {
			return wire, nil
		}
		return string(valueDesc.Name()), nil
	}))
}

// ProtoEnumResolverFromFileDescriptorSet builds a dynamic protobuf resolver
// from fds.
//
// A nil fds returns an empty resolver with nil error. Non-nil descriptor sets
// must be self-contained enough for [protodesc.NewFiles] to resolve imports.
// Reading .proto files, fetching remote descriptors, invoking compilers, and
// merging descriptor sets are application responsibilities.
func ProtoEnumResolverFromFileDescriptorSet(fds *descriptorpb.FileDescriptorSet) (ProtoEnumResolver, error) {
	if fds == nil {
		return dynamicpb.NewTypes(nil), nil
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, err
	}
	return dynamicpb.NewTypes(files), nil
}

// ComposeProtoEnumResolvers returns a resolver that tries resolvers in order.
//
// Nil resolvers are skipped. Lookup continues only when a resolver returns the
// exact [protoregistry.NotFound] sentinel; wrapped NotFound errors are returned
// as ordinary errors. If no resolver finds a type, the composed resolver returns
// exact [protoregistry.NotFound].
func ComposeProtoEnumResolvers(resolvers ...ProtoEnumResolver) ProtoEnumResolver {
	active := make([]ProtoEnumResolver, 0, len(resolvers))
	for _, resolver := range resolvers {
		if !isNilResolver(resolver) {
			active = append(active, resolver)
		}
	}
	if len(active) == 0 {
		// Keep ComposeProtoEnumResolvers usable as a resolver even when it is
		// empty: direct Find* calls return exact NotFound instead of panicking.
		return compositeResolver{resolvers: nil}
	}
	if len(active) == 1 {
		return active[0]
	}
	return compositeResolver{resolvers: active}
}

type compositeResolver struct {
	resolvers []ProtoEnumResolver
}

func (r compositeResolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	return find(r.resolvers, func(resolver ProtoEnumResolver) (protoreflect.MessageType, error) {
		return resolver.FindMessageByName(name)
	})
}

func (r compositeResolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	return find(r.resolvers, func(resolver ProtoEnumResolver) (protoreflect.MessageType, error) {
		return resolver.FindMessageByURL(url)
	})
}

func (r compositeResolver) FindExtensionByName(name protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return find(r.resolvers, func(resolver ProtoEnumResolver) (protoreflect.ExtensionType, error) {
		return resolver.FindExtensionByName(name)
	})
}

func (r compositeResolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return find(r.resolvers, func(resolver ProtoEnumResolver) (protoreflect.ExtensionType, error) {
		return resolver.FindExtensionByNumber(message, field)
	})
}

func (r compositeResolver) FindEnumByName(name protoreflect.FullName) (protoreflect.EnumType, error) {
	return find(r.resolvers, func(resolver ProtoEnumResolver) (protoreflect.EnumType, error) {
		return resolver.FindEnumByName(name)
	})
}

func find[T any](resolvers []ProtoEnumResolver, lookup func(ProtoEnumResolver) (T, error)) (T, error) {
	var zero T
	for _, resolver := range resolvers {
		v, err := lookup(resolver)
		if err == nil {
			return v, nil
		}
		if !isExactNotFound(err) {
			return zero, err
		}
	}
	return zero, protoregistry.NotFound
}

func stringWire(value spanner.GenericColumnValue, code sppb.TypeCode) (string, error) {
	if _, ok := value.Value.GetKind().(*structpb.Value_StringValue); !ok {
		return "", fmt.Errorf("%w: %v value kind %T", spanvalue.ErrMalformedWire, code, value.Value.GetKind())
	}
	return value.Value.GetStringValue(), nil
}

// unresolvedFallthrough reports a descriptor resolution failure to
// onUnresolved when set and returns the error the plugin should surface: the
// handler's non-nil error, or [spanvalue.ErrFallthrough] otherwise.
func unresolvedFallthrough(onUnresolved func(typeFQN string, code sppb.TypeCode) error, typeName protoreflect.FullName, code sppb.TypeCode) error {
	if onUnresolved != nil {
		if err := onUnresolved(string(typeName), code); err != nil {
			return err
		}
	}
	return spanvalue.ErrFallthrough
}

func isExactNotFound(err error) bool {
	// Resolver contracts require the exact sentinel so wrapped NotFound errors
	// remain real errors instead of accidental fallback.
	return err == protoregistry.NotFound //nolint:errorlint
}

// fallthroughPlugin defers every value to the rest of the chain.
func fallthroughPlugin(spanvalue.Formatter, spanner.GenericColumnValue, bool) (string, error) {
	return "", spanvalue.ErrFallthrough
}

func isNilResolver(resolver any) bool {
	if resolver == nil {
		return true
	}
	v := reflect.ValueOf(resolver)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
