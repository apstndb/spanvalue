// Package gospanner wires [github.com/googleapis/go-sql-spanner] query execution
// to [github.com/apstndb/spanvalue/dbsqlrows] export helpers.
//
// Import this nested module only when the application already depends on
// go-sql-spanner. The root github.com/apstndb/spanvalue module does not
// require go-sql-spanner; use [github.com/apstndb/spanvalue/dbsqlrows] alone
// when you open [*database/sql.Rows] yourself.
//
// See README.md in this directory.
package gospanner
