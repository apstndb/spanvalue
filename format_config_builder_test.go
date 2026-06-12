package spanvalue_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"google.golang.org/protobuf/types/known/structpb"
)

// builderStructField is a minimal Formatter-based struct field callback for
// builder tests: format the field value through the chain, no name decoration.
func builderStructField(f spanvalue.Formatter, sf *sppb.StructType_Field, value *structpb.Value) (string, error) {
	return f.FormatColumn(spanner.GenericColumnValue{Type: sf.GetType(), Value: value}, false)
}

func TestNewFormatConfigValidation(t *testing.T) {
	t.Parallel()

	nullStr := spanvalue.WithNullString("NULL")
	array := spanvalue.WithArrayFormat(spanvalue.FormatUntypedArray)
	structFmt := spanvalue.WithStructFormat(builderStructField, spanvalue.FormatTupleStruct)
	scalar := spanvalue.WithScalarFormatter(spanvalue.FormatNullableSpannerCLICompatible)

	tests := []struct {
		name    string
		opts    []spanvalue.FormatConfigOption
		wantErr error
	}{
		{
			name: "complete",
			opts: []spanvalue.FormatConfigOption{nullStr, array, structFmt, scalar},
		},
		{
			name:    "missing scalar formatter",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, structFmt},
			wantErr: spanvalue.ErrScalarFormatterRequired,
		},
		{
			name:    "nil scalar formatter",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, structFmt, spanvalue.WithScalarFormatter(nil)},
			wantErr: spanvalue.ErrScalarFormatterRequired,
		},
		{
			name:    "missing array format",
			opts:    []spanvalue.FormatConfigOption{nullStr, structFmt, scalar},
			wantErr: spanvalue.ErrArrayFormatRequired,
		},
		{
			name:    "nil array format",
			opts:    []spanvalue.FormatConfigOption{nullStr, spanvalue.WithArrayFormat(nil), structFmt, scalar},
			wantErr: spanvalue.ErrArrayFormatRequired,
		},
		{
			name:    "missing struct format",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, scalar},
			wantErr: spanvalue.ErrStructFormatRequired,
		},
		{
			name:    "nil struct field",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, spanvalue.WithStructFormat(nil, spanvalue.FormatTupleStruct), scalar},
			wantErr: spanvalue.ErrStructFormatRequired,
		},
		{
			name:    "nil struct paren",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, spanvalue.WithStructFormat(builderStructField, nil), scalar},
			wantErr: spanvalue.ErrStructFormatRequired,
		},
		{
			name:    "missing null string",
			opts:    []spanvalue.FormatConfigOption{array, structFmt, scalar},
			wantErr: spanvalue.ErrEmptyNullString,
		},
		{
			name: "a plugin alone satisfies no required handler",
			opts: []spanvalue.FormatConfigOption{
				nullStr,
				spanvalue.WithPlugin(constPlugin("total")),
			},
			wantErr: spanvalue.ErrScalarFormatterRequired,
		},
		{
			name:    "nil override plugin",
			opts:    []spanvalue.FormatConfigOption{nullStr, array, structFmt, scalar, spanvalue.WithPlugin(nil)},
			wantErr: spanvalue.ErrNilFormatComplexPlugin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fc, err := spanvalue.NewFormatConfig(tt.opts...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewFormatConfig() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if fc != nil {
					t.Fatalf("NewFormatConfig() = %v, want nil config on error", fc)
				}
				return
			}
			if err := fc.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestNewFormatConfigPluginOrder(t *testing.T) {
	t.Parallel()

	first := spanvalue.PluginForTypeCode(sppb.TypeCode_INT64, constPlugin("first"))
	second := spanvalue.PluginForTypeCode(sppb.TypeCode_INT64, constPlugin("second"))
	arrayOverride := spanvalue.PluginForTypeCode(sppb.TypeCode_ARRAY,
		spanvalue.PluginSkippingNull(constPlugin("array-override")))

	fc, err := spanvalue.NewFormatConfig(
		spanvalue.WithNullString("NULL"),
		spanvalue.WithPlugin(first),
		spanvalue.WithPlugin(second), // most recent registration runs first
		spanvalue.WithPlugin(arrayOverride),
		spanvalue.WithArrayFormat(spanvalue.FormatUntypedArray),
		spanvalue.WithStructFormat(builderStructField, spanvalue.FormatTupleStruct),
		spanvalue.WithScalarFormatter(spanvalue.FormatNullableSpannerCLICompatible),
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := fc.FormatToplevelColumn(gcvctor.Int64Value(1))
	if err != nil || got != "second" {
		t.Errorf("INT64 = (%q, %v), want (second, nil): most recent WithPlugin runs first", got, err)
	}

	// Overrides run before the WithArrayFormat handler.
	arr, err := gcvctor.ArrayValue(gcvctor.Int64Value(1))
	if err != nil {
		t.Fatal(err)
	}
	got, err = fc.FormatToplevelColumn(arr)
	if err != nil || got != "array-override" {
		t.Errorf("ARRAY = (%q, %v), want (array-override, nil): overrides precede handlers", got, err)
	}

	// Values no override claims reach the canonical handlers.
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("STRING = (%q, %v), want (s, nil) from the scalar tail", got, err)
	}
}
