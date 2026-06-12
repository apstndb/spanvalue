package spanvalue

import (
	"errors"
	"slices"
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
			name: "empty plugin chain",
			mutate: func(fc *FormatConfig) {
				fc.FormatComplexPlugins = nil
			},
			wantErr: ErrEmptyFormatComplexPlugins,
		},
		{
			name: "nil plugin in format complex plugins",
			mutate: func(fc *FormatConfig) {
				fc.FormatComplexPlugins = append(slices.Clone(fc.FormatComplexPlugins), nil)
			},
			wantErr: ErrNilFormatComplexPlugin,
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

func TestFormatConfigClone(t *testing.T) {
	t.Parallel()

	plugin := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "", ErrFallthrough
	})
	original := &FormatConfig{
		NullString:           "null",
		FormatComplexPlugins: []FormatComplexFunc{plugin},
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

func TestFormatConfigWithComplexPlugin(t *testing.T) {
	t.Parallel()

	plugin := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "plugin", nil
	})
	other := FormatComplexFunc(func(Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return "other", nil
	})

	t.Run("nil receiver", func(t *testing.T) {
		t.Parallel()
		if got := (*FormatConfig)(nil).WithComplexPlugin(plugin); got != nil {
			t.Fatalf("WithComplexPlugin on nil receiver = %v, want nil", got)
		}
	})

	t.Run("nil plugin panics", func(t *testing.T) {
		t.Parallel()
		fc := SimpleFormatConfig()
		defer func() {
			if recover() == nil {
				t.Fatal("WithComplexPlugin(nil) did not panic")
			}
		}()
		_ = fc.WithComplexPlugin(nil)
	})

	t.Run("preset singleton unchanged", func(t *testing.T) {
		t.Parallel()
		preset := SimpleFormatConfig()
		before := len(preset.FormatComplexPlugins)
		got := preset.WithComplexPlugin(plugin)
		if len(preset.FormatComplexPlugins) != before {
			t.Fatalf("preset len = %d, want %d unchanged", len(preset.FormatComplexPlugins), before)
		}
		if got == preset {
			t.Fatal("WithComplexPlugin returned preset singleton")
		}
		if len(got.FormatComplexPlugins) != before+1 {
			t.Fatalf("len(FormatComplexPlugins) = %d, want %d", len(got.FormatComplexPlugins), before+1)
		}
		if got.FormatComplexPlugins[0] == nil {
			t.Fatal("prepended nil plugin")
		}
	})

	t.Run("chain", func(t *testing.T) {
		t.Parallel()
		preset := SimpleFormatConfig()
		before := len(preset.FormatComplexPlugins)
		got := preset.WithComplexPlugin(plugin).WithComplexPlugin(other)
		if len(preset.FormatComplexPlugins) != before {
			t.Fatalf("preset len = %d, want %d unchanged", len(preset.FormatComplexPlugins), before)
		}
		if len(got.FormatComplexPlugins) != before+2 {
			t.Fatalf("len(FormatComplexPlugins) = %d, want %d", len(got.FormatComplexPlugins), before+2)
		}
		if got.FormatComplexPlugins[0] == nil || got.FormatComplexPlugins[1] == nil {
			t.Fatal("chained prepend left nil plugin slot")
		}
		// Most recent WithComplexPlugin call prepends first (other before plugin).
		if s, err := got.FormatComplexPlugins[0](nil, spanner.GenericColumnValue{}, false); err != nil || s != "other" {
			t.Fatalf("first plugin = %q, want other", s)
		}
		if s, err := got.FormatComplexPlugins[1](nil, spanner.GenericColumnValue{}, false); err != nil || s != "plugin" {
			t.Fatalf("second plugin = %q, want plugin", s)
		}
		if err := got.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
	})

	t.Run("hand-built clone isolation", func(t *testing.T) {
		t.Parallel()
		original := &FormatConfig{
			NullString:           "NULL",
			FormatComplexPlugins: []FormatComplexFunc{plugin},
		}
		got := original.WithComplexPlugin(other)
		if got == original {
			t.Fatal("WithComplexPlugin returned same pointer as original")
		}
		if len(original.FormatComplexPlugins) != 1 {
			t.Fatalf("original len = %d, want 1", len(original.FormatComplexPlugins))
		}
		if len(got.FormatComplexPlugins) != 2 {
			t.Fatalf("got len = %d, want 2", len(got.FormatComplexPlugins))
		}
		got.FormatComplexPlugins[0] = nil
		if original.FormatComplexPlugins[0] == nil {
			t.Fatal("mutating returned config changed original plugin slice element")
		}
	})
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
