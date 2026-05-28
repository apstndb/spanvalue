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
		FormatStruct:         TypedStructFormat(),
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

	clone.FormatComplexPlugins[0] = nil
	if original.FormatComplexPlugins[0] == nil {
		t.Fatal("mutating clone.FormatComplexPlugins element changed original")
	}

	if got := (*FormatConfig)(nil).Clone(); got != nil {
		t.Fatalf("Clone(nil) = %v, want nil", got)
	}
}

func TestFormatConfigClonePluginAppendIsolation(t *testing.T) {
	t.Parallel()

	plugin := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "", ErrFallthrough
	})
	other := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "other", nil
	})

	t.Run("non-empty plugins", func(t *testing.T) {
		t.Parallel()

		original := &FormatConfig{
			FormatComplexPlugins: []FormatComplexFunc{plugin},
		}
		clone := original.Clone()

		clone.FormatComplexPlugins = append(clone.FormatComplexPlugins, other)
		if len(original.FormatComplexPlugins) != 1 {
			t.Fatalf("append on clone changed original len: got %d want 1", len(original.FormatComplexPlugins))
		}
		if len(clone.FormatComplexPlugins) != 2 {
			t.Fatalf("clone len = %d, want 2", len(clone.FormatComplexPlugins))
		}
	})

	t.Run("empty non-nil plugins", func(t *testing.T) {
		t.Parallel()

		original := &FormatConfig{
			FormatComplexPlugins: make([]FormatComplexFunc, 0, 2),
		}
		clone := original.Clone()

		clone.FormatComplexPlugins = append(clone.FormatComplexPlugins, other)
		if len(original.FormatComplexPlugins) != 0 {
			t.Fatalf("append on clone changed original len: got %d want 0", len(original.FormatComplexPlugins))
		}
		if len(clone.FormatComplexPlugins) != 1 {
			t.Fatalf("clone len = %d, want 1", len(clone.FormatComplexPlugins))
		}
	})
}
