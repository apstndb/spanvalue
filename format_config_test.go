package spanvalue

import (
	"testing"

	"cloud.google.com/go/spanner"
)

func TestFormatConfigClone(t *testing.T) {
	t.Parallel()

	plugin := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "", ErrFallthrough
	})
	original := &FormatConfig{
		NullString:           "null",
		FormatArray:          FormatCompactArray,
		FormatStruct:         FormatTypedStruct,
		FormatComplexPlugins: []FormatComplexFunc{plugin},
		FormatNullable:       FormatNullableSpannerCLICompatible,
	}

	clone := original.Clone()
	if clone == original {
		t.Fatal("Clone() returned the same pointer")
	}
	if clone.NullString != original.NullString {
		t.Fatalf("NullString = %q, want %q", clone.NullString, original.NullString)
	}
	if len(clone.FormatComplexPlugins) != len(original.FormatComplexPlugins) {
		t.Fatalf("len(FormatComplexPlugins) = %d, want %d", len(clone.FormatComplexPlugins), len(original.FormatComplexPlugins))
	}

	clone.NullString = "changed"
	clone.FormatComplexPlugins = nil
	if original.NullString == "changed" {
		t.Fatal("mutating clone.NullString changed original")
	}
	if original.FormatComplexPlugins == nil {
		t.Fatal("mutating clone.FormatComplexPlugins replaced original slice")
	}

	if got := (*FormatConfig)(nil).Clone(); got != nil {
		t.Fatalf("Clone(nil) = %v, want nil", got)
	}
}
