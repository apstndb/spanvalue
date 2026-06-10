package gospanner

import (
	"context"
	"database/sql"
	"errors"

	spannerdriver "github.com/googleapis/go-sql-spanner"

	"github.com/apstndb/spanvalue/dbsqlrows"
)

var errNilDB = errors.New("nil *sql.DB")

// DefaultExecOptions returns the recommended go-sql-spanner configuration for
// proto-decoded GCV export with a leading metadata pseudo result set
// ([spannerdriver.ExecOptions.ReturnResultSetMetadata]). ReturnResultSetStats
// is false so callers can read stats after export (for example spannersh
// execution summaries) or set [dbsqlrows.SQLRowsConfig.ReadResultSetStats].
func DefaultExecOptions() spannerdriver.ExecOptions {
	return spannerdriver.ExecOptions{
		DecodeOption:            spannerdriver.DecodeOptionProto,
		ReturnResultSetMetadata: true,
		ReturnResultSetStats:    false,
	}
}

// QueryExport runs db.QueryContext with [DefaultExecOptions] and exports the
// result via [dbsqlrows.WriteRows]. It closes rows before returning.
func QueryExport(
	ctx context.Context,
	db *sql.DB,
	query string,
	args []any,
	w dbsqlrows.GCVStreamWriter,
	cfg dbsqlrows.SQLRowsConfig,
) (*dbsqlrows.SQLRowsResult, error) {
	return QueryExportWithOptions(ctx, db, query, args, w, cfg, DefaultExecOptions())
}

// QueryExportWithOptions is [QueryExport] with explicit driver [spannerdriver.ExecOptions].
func QueryExportWithOptions(
	ctx context.Context,
	db *sql.DB,
	query string,
	args []any,
	w dbsqlrows.GCVStreamWriter,
	cfg dbsqlrows.SQLRowsConfig,
	opts spannerdriver.ExecOptions,
) (*dbsqlrows.SQLRowsResult, error) {
	if db == nil {
		return nil, errNilDB
	}
	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, opts)
	queryArgs = append(queryArgs, args...)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	result, writeErr := dbsqlrows.WriteRows(rows, w, cfg)
	closeErr := rows.Close()
	if writeErr != nil {
		return result, writeErr
	}
	return result, closeErr
}
