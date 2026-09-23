package dbsqlrows

import "database/sql"

// rowsFacade mirrors the test seam pattern in writer/row_iterator.go so unit
// tests can stub Next, NextResultSet, Scan, and column layout without a live DB.
type rowsFacade interface {
	next() bool
	nextResultSet() bool
	scan(dest ...any) error
	columnNames() ([]string, error)
	columnCount() (int, error)
	err() error
}

type sqlRowsFacade struct {
	*sql.Rows
}

func (f sqlRowsFacade) next() bool {
	return f.Next()
}

func (f sqlRowsFacade) nextResultSet() bool {
	return f.NextResultSet()
}

func (f sqlRowsFacade) scan(dest ...any) error {
	return f.Scan(dest...)
}

func (f sqlRowsFacade) columnNames() ([]string, error) {
	return f.Columns()
}

func (f sqlRowsFacade) columnCount() (int, error) {
	cols, err := f.columnNames()
	if err != nil {
		return 0, err
	}
	return len(cols), nil
}

func (f sqlRowsFacade) err() error {
	return f.Err()
}
