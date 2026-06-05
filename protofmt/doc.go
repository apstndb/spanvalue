// Package protofmt provides descriptor-aware PROTO and ENUM formatting plugins
// for spanvalue format configs.
//
// The plugins in this package are opt-in. They are intended for display paths
// where protobuf descriptors are available, such as CLI table output. They do
// not change spanvalue preset defaults and do not replace descriptor-free SQL
// literal fallbacks such as [github.com/apstndb/spanvalue.FormatProtoAsCast] and
// [github.com/apstndb/spanvalue.FormatEnumAsCast].
//
// Generated protobuf types can be resolved through
// [google.golang.org/protobuf/reflect/protoregistry.GlobalTypes] when their
// generated packages are imported and linked into the binary, including by
// blank import.
//
// To enable descriptor-aware display, clone an existing spanvalue format preset
// and prepend [FormatProtoTextValue] and [FormatEnumNameValue] before the
// preset's existing plugins. See the package examples for the minimal pattern.
package protofmt
