package gospanner

import (
	"context"
	"database/sql"
	"errors"

	spannerdriver "github.com/googleapis/go-sql-spanner"

	"github.com/apstndb/spanvalue/dbsqlrows"
)

// ErrNilDB reports that [QueryExport] or [QueryExportWithOptions] was called
// with a nil *sql.DB.
var ErrNilDB = errors.New("nil *sql.DB")

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
//
// The driver [spannerdriver.ExecOptions] value is prepended as the first
// query argument, before args. That is the go-sql-spanner convention; an
// argument-count error often means that leading value was forgotten or duplicated.
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
		return nil, ErrNilDB
	}
	if w == nil {
		return nil, dbsqlrows.ErrNilWriter
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
