// Package protofmt provides descriptor-aware PROTO and ENUM formatting plugins
// for spanvalue format configs.
//
// The plugins in this package are opt-in. They are intended for display paths
// where protobuf descriptors are available, such as CLI table output. They do
// not change spanvalue preset defaults and do not replace descriptor-free SQL
// literal fallbacks such as spanvalue.FormatProtoAsCast and
// spanvalue.FormatEnumAsCast.
package protofmt
