package dbsqlrows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// This models go-sql-spanner's emptyRows data phase without a Spanner service:
// zero metadata fields, a synthetic affected_rows column, and immediate EOF.
type ddlEmptyConnector struct{}

func (ddlEmptyConnector) Connect(context.Context) (driver.Conn, error) { return ddlEmptyConn{}, nil }
func (ddlEmptyConnector) Driver() driver.Driver                        { return ddlEmptyDriver{} }

type ddlEmptyDriver struct{}

func (ddlEmptyDriver) Open(string) (driver.Conn, error) { return ddlEmptyConn{}, nil }

type ddlEmptyConn struct{}

func (ddlEmptyConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not used")
}
func (ddlEmptyConn) Close() error { return nil }
func (ddlEmptyConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transaction not used")
}
func (ddlEmptyConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &ddlEmptyRows{}, nil
}

type ddlEmptyRows struct{ set int }

func (r *ddlEmptyRows) Columns() []string {
	if r.set == 0 {
		return []string{"affected_rows"}
	}
	return []string{"stats"}
}
func (*ddlEmptyRows) Close() error              { return nil }
func (*ddlEmptyRows) Next([]driver.Value) error { return io.EOF }
func (r *ddlEmptyRows) HasNextResultSet() bool  { return r.set == 0 }
func (r *ddlEmptyRows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.set++
	return nil
}

func TestRunRowsAtDataDDLEmptyRows(t *testing.T) {
	t.Parallel()

	db := sql.OpenDB(ddlEmptyConnector{})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	rows, err := db.QueryContext(t.Context(), "CREATE TABLE t (id INT64) PRIMARY KEY (id)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	})

	var prepared, written, finished int
	got, err := RunRowsAtData(rows, metadataWithNames(), SQLRowsHooks{
		PrepareMetadata: func(*sppb.ResultSetMetadata) error { prepared++; return nil },
		WriteDataRow:    func([]spanner.GenericColumnValue) error { written++; return nil },
		Finish:          func(*SQLRowsResult) error { finished++; return nil },
	}, SQLRowsConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 0 || prepared != 1 || written != 0 || finished != 1 {
		t.Fatalf("result=%#v prepared=%d written=%d finished=%d", got, prepared, written, finished)
	}
	if !rows.NextResultSet() {
		t.Fatalf("stats result set unavailable after DDL data phase: %v", rows.Err())
	}
	columns, err := rows.Columns()
	if err != nil || len(columns) != 1 || columns[0] != "stats" {
		t.Fatalf("stats columns = %q, %v", columns, err)
	}
}
