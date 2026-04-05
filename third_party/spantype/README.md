# spantype

`github.com/apstndb/spantype` provides two related packages for working with Cloud Spanner types:

- `spantype`: format `google.spanner.v1.Type` values for logs, errors, and debugging.
- `typector`: construct `*spannerpb.Type` and `*spannerpb.StructType_Field` values for tests and helpers.

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spantype.svg)](https://pkg.go.dev/github.com/apstndb/spantype)

## Packages

### `spantype`

The root package formats Spanner types with configurable verbosity. For a type like `STRUCT<arr ARRAY<STRUCT<n INT64>>, proto PROTO<examples.Book>>`, the helpers differ like this:

| Function | Intended use | Example output |
| --- | --- | --- |
| `FormatTypeSimplest` | Very compact summaries such as schema overviews | `STRUCT` |
| `FormatTypeSimple` | Compact logs; top-level `STRUCT`s are still collapsed to `STRUCT` | `STRUCT` |
| `FormatTypeNormal` | Default structured output without field names | `STRUCT<ARRAY<STRUCT<INT64>>, Book>` |
| `FormatTypeVerbose` | Human-facing diagnostics with struct field names | `STRUCT<arr ARRAY<STRUCT<n INT64>>, proto examples.Book>` |
| `FormatTypeMoreVerbose` | Errors and debugging where `PROTO` / `ENUM` kind should stay explicit | `STRUCT<arr ARRAY<STRUCT<n INT64>>, proto PROTO<examples.Book>>` |

If you need custom behavior, call `FormatType` with `FormatOption`.

### `typector`

`typector` is a constructor helper package for building Spanner type values.

- Use `CodeToSimpleType` when you already have a `spannerpb.TypeCode`.
- Use shorthand constructors such as `Int64()`, `String()`, and `UUID()` for common scalar types.
- Use `ElemCodeToArrayType` / `ElemTypeToArrayType` for arrays.
- Use `FQNToProtoType` / `FQNToEnumType` for `PROTO` and `ENUM`, which require a fully-qualified name.
- Prefer `...Code...` forms when your input is a type code, and `...Type...` forms when you already have `*spannerpb.Type`.

## CLI Example

[`./cmd/spantype`](./cmd/spantype) is a small example program that reads protobuf JSON from stdin:

```shell
echo '{"fields":[{"name":"n","type":{"code":"INT64"}}]}' | go run ./cmd/spantype --mode=verbose
```

Supported modes are `simplest`, `simple`, `normal`, `verbose`, and `more`.

## Development

```shell
go test ./...
go build ./...
go vet ./...
```
