package dbsqlrows

import (
	"database/sql"
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// ReadMetadataAndAdvanceToData reads the metadata pseudo-row from rows and advances
// to the data result set (go-sql-spanner ReturnResultSetMetadata). Use before
// [ExportRowsAtData] or custom rendering when metadata is consumed outside export.
//
// If there is no metadata row, returns ok=false and err=rows.Err() (nil on clean EOF).
func ReadMetadataAndAdvanceToData(rows *sql.Rows) (*sppb.ResultSetMetadata, bool, error) {
	if rows == nil {
		return nil, false, ErrNilRows
	}
	return readMetadataAndAdvanceToData(sqlRowsFacade{rows})
}

func readMetadataAndAdvanceToData(fac rowsFacade) (*sppb.ResultSetMetadata, bool, error) {
	if !fac.next() {
		return nil, false, fac.err()
	}
	var md *sppb.ResultSetMetadata
	if err := fac.scan(&md); err != nil {
		return nil, false, err
	}
	if !fac.nextResultSet() {
		return nil, false, fmt.Errorf("expected data rows result set after metadata")
	}
	return md, true, nil
}
