package spanvalue

// PostgreSQLLiteralFormatConfig returns a FormatConfig intended for PostgreSQL-dialect
// SQL INSERT value literals (single-quoted strings, PG DATE/TIMESTAMP/JSONB forms,
// INTERVAL '…' syntax, and related annotations).
//
// # WIP (#126)
//
// This scaffold is not production-ready. Today it only applies single-quoted string
// and bytes delimiters via [LiteralFormatConfigWithSingleQuotedLiterals]; scalar
// type annotations still follow the GoogleSQL literal preset (for example
// TIMESTAMP "…", NUMERIC "…", CAST(… AS INTERVAL)).
//
// Open design questions:
//   - Should PG literals live entirely in spanvalue, or delegate probing to spanpg?
//   - How should PROTO/ENUM and descriptor-backed types render under PostgreSQL?
//   - Should [github.com/apstndb/spanvalue/writer.SQLInsertWriter] auto-select this preset when
//     [github.com/apstndb/spanvalue/writer.WithSQLDialect] is called with databasepb.DatabaseDialect_POSTGRESQL and no
//     explicit [github.com/apstndb/spanvalue/writer.WithFormatter] was provided?
//   - ARRAY/STRUCT nesting: PG array literal syntax vs typed GoogleSQL ARRAY<…>[…]?
//
// Track progress at https://github.com/apstndb/spanvalue/issues/126 .
func PostgreSQLLiteralFormatConfig() *FormatConfig {
	return LiteralFormatConfigWithSingleQuotedLiterals()
}
