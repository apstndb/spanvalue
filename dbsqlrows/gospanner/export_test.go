package gospanner

import (
	"database/sql"
	"errors"
	"testing"

	spannerdriver "github.com/googleapis/go-sql-spanner"

	"github.com/apstndb/spanvalue/dbsqlrows"
)

func TestDefaultExecOptions(t *testing.T) {
	t.Parallel()

	opts := DefaultExecOptions()
	if opts.DecodeOption != spannerdriver.DecodeOptionProto {
		t.Fatalf("DecodeOption = %v, want DecodeOptionProto", opts.DecodeOption)
	}
	if !opts.ReturnResultSetMetadata {
		t.Fatal("ReturnResultSetMetadata = false, want true")
	}
	if opts.ReturnResultSetStats {
		t.Fatal("ReturnResultSetStats = true, want false")
	}
}

func TestQueryExport_nilDB(t *testing.T) {
	t.Parallel()

	_, err := QueryExport(t.Context(), nil, "SELECT 1", nil, nil, dbsqlrows.SQLRowsConfig{})
	if !errors.Is(err, ErrNilDB) {
		t.Fatalf("error = %v, want %v", err, ErrNilDB)
	}
}

func TestQueryExport_nilWriter(t *testing.T) {
	t.Parallel()

	db := &sql.DB{}
	_, err := QueryExport(t.Context(), db, "SELECT 1", nil, nil, dbsqlrows.SQLRowsConfig{})
	if !errors.Is(err, dbsqlrows.ErrNilWriter) {
		t.Fatalf("error = %v, want ErrNilWriter", err)
	}
}
