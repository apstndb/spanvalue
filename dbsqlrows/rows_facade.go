package dbsqlrows

import "database/sql"

// rowsFacade mirrors the test seam pattern in writer/row_iterator.go so unit
// tests can stub Next, NextResultSet, Scan, and column layout without a live DB.
type rowsFacade interface {
	next() bool
	nextResultSet() bool
	scan(dest ...any) error
	columnCount() (int, error)
	err() error
}

type sqlRowsFacade struct {
	*sql.Rows
}

func (f sqlRowsFacade) next() bool {
	return f.Rows.Next()
}

func (f sqlRowsFacade) nextResultSet() bool {
	return f.Rows.NextResultSet()
}

func (f sqlRowsFacade) scan(dest ...any) error {
	return f.Rows.Scan(dest...)
}

func (f sqlRowsFacade) columnCount() (int, error) {
	cols, err := f.Rows.Columns()
	if err != nil {
		return 0, err
	}
	return len(cols), nil
}

func (f sqlRowsFacade) err() error {
	return f.Rows.Err()
}
