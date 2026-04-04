package spanvalue

import (
	"fmt"
	"slices"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/samber/lo"
)

// ColumnNames returns the names of the provided fields. Unnamed fields are kept
// as empty strings unless a non-nil namer is provided, in which case the namer
// is used to generate names for unnamed fields. If a non-nil UnnamedFieldNamer
// returns an empty string or repeatedly returns colliding names such that a
// unique column name cannot be chosen, ColumnNames returns a non-nil error
// describing the contract violation.
func ColumnNames(fields []*sppb.StructType_Field, namer UnnamedFieldNamer) ([]string, error) {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.GetName()
	}
	return resolveColumnNamesInPlace(names, namer)
}

// FormatRowColumns formats a row represented as column names plus GCV values.
// The column names are validated for shape compatibility, but the formatted cell
// values come from the GCVs themselves.
func FormatRowColumns(fc *FormatConfig, columnNames []string, values []spanner.GenericColumnValue) ([]string, error) {
	if len(columnNames) != len(values) {
		return nil, fmt.Errorf("len(columnNames)=%v != len(values)=%v", len(columnNames), len(values))
	}
	return fc.formatColumns(values)
}

// FormatRowJSONObjectFromColumns formats a row represented as column names plus
// GCV values into a JSON object string. The provided FormatConfig must emit
// standalone JSON values per column (for example, as configured by
// JSONFormatConfig()), otherwise the assembled object may be syntactically
// invalid JSON.
func FormatRowJSONObjectFromColumns(fc *FormatConfig, columnNames []string, values []spanner.GenericColumnValue, namer UnnamedFieldNamer) (string, error) {
	formattedValues, err := FormatRowColumns(fc, columnNames, values)
	if err != nil {
		return "", err
	}
	return assembleJSONObject(columnNames, formattedValues, namer)
}

func (fc *FormatConfig) formatColumns(values []spanner.GenericColumnValue) ([]string, error) {
	return lo.MapErr(values, func(gcv spanner.GenericColumnValue, _ int) (string, error) {
		return fc.FormatColumn(gcv, true)
	})
}

func resolveColumnNames(columnNames []string, namer UnnamedFieldNamer) ([]string, error) {
	if namer == nil {
		return columnNames, nil
	}
	return resolveColumnNamesInPlace(slices.Clone(columnNames), namer)
}

func resolveColumnNamesInPlace(names []string, namer UnnamedFieldNamer) ([]string, error) {
	if namer == nil {
		return names, nil
	}

	usedNames := make(map[string]bool, len(names))
	for _, name := range names {
		if name != "" {
			usedNames[name] = true
		}
	}

	autoIdx := 0
	var attempted map[string]bool // lazily allocated, reused via clear()
	for i, name := range names {
		if name != "" {
			continue
		}
		if attempted == nil {
			attempted = make(map[string]bool)
		} else {
			clear(attempted)
		}
		for {
			name = namer(autoIdx)
			autoIdx++
			if name == "" {
				return nil, fmt.Errorf("unnamed field namer returned empty string (field index %d, generated index %d)", i, autoIdx-1)
			}
			if !usedNames[name] {
				break
			}
			if attempted[name] {
				return nil, fmt.Errorf("unnamed field namer returned repeated colliding name %q (field index %d, generated index %d)", name, i, autoIdx-1)
			}
			attempted[name] = true
		}
		names[i] = name
		usedNames[name] = true
	}

	return names, nil
}
