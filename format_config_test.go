package spanvalue

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestFormatConfigValidate_presets(t *testing.T) {
	t.Parallel()

	presets := []*FormatConfig{
		LiteralFormatConfig(),
		SimpleFormatConfig(),
		SpannerCLICompatibleFormatConfig(),
		JSONFormatConfig(),
	}
	for i, fc := range presets {
		if err := fc.Validate(); err != nil {
			t.Errorf("preset[%d].Validate() = %v, want nil", i, err)
		}
	}
}

func TestFormatConfigValidate_handBuiltInvalid(t *testing.T) {
	t.Parallel()

	valid := &FormatConfig{
		NullString:           "NULL",
		FormatArray:          FormatCompactArray,
		FormatStruct:         TypedStructFormat(),
		FormatNullable:       formatNullableValueSimple,
		FormatComplexPlugins: []FormatComplexFunc{FormatSimpleValue},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid.Validate() = %v, want nil", err)
	}

	tests := []struct {
		name    string
		mutate  func(*FormatConfig)
		wantErr error
	}{
		{
			name:    "nil config",
			wantErr: ErrNilFormatConfig,
		},
		{
			name: "empty null string",
			mutate: func(fc *FormatConfig) {
				fc.NullString = ""
			},
			wantErr: ErrEmptyNullString,
		},
		{
			name: "nil format array",
			mutate: func(fc *FormatConfig) {
				fc.FormatArray = nil
			},
			wantErr: ErrNilFormatArray,
		},
		{
			name: "nil format struct field",
			mutate: func(fc *FormatConfig) {
				fc.FormatStruct.FormatStructField = nil
			},
			wantErr: ErrNilFormatStructField,
		},
		{
			name: "nil format struct paren",
			mutate: func(fc *FormatConfig) {
				fc.FormatStruct.FormatStructParen = nil
			},
			wantErr: ErrNilFormatStructParen,
		},
		{
			name: "nil format nullable without scalar plugins",
			mutate: func(fc *FormatConfig) {
				fc.FormatNullable = nil
				fc.FormatComplexPlugins = nil
			},
			wantErr: ErrFormatNullableRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var fc *FormatConfig
			if tt.name == "nil config" {
				fc = nil
			} else {
				fc = valid.Clone()
				tt.mutate(fc)
			}
			err := fc.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestFormatConfigValidate_nilFormatNullableWithScalarPlugins(t *testing.T) {
	t.Parallel()

	fc := SimpleFormatConfig()
	fc.FormatNullable = nil
	if err := fc.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil when scalar plugins remain", err)
	}
}

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
