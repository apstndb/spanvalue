package spanvalue

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// spannerCLICompatibleFormatConfig is a shared singleton used by convenience functions
// (FormatRowSpannerCLICompatible, FormatColumnSpannerCLICompatible) to avoid per-call allocation.
// Do not mutate: it is shared across all callers.
var spannerCLICompatibleFormatConfig = SpannerCLICompatibleFormatConfig()

// SpannerCLICompatibleFormatConfig returns a new FormatConfig that matches
// the output format of the official [spanner-cli] tool (bracket-style STRUCT
// fields in arrays, for example [[1, east]] for `ARRAY<STRUCT<...>>`).
//
// Tuple-style STRUCT parentheses such as [(1, east)] are not spanner-cli output.
// To keep Spanner CLI scalar formatting but render STRUCT with [FormatTupleStruct],
// prepend a [PluginForStruct] override (it runs before the preset's STRUCT handler
// and claims non-NULL STRUCT values):
//
//	fc := SpannerCLICompatibleFormatConfig().WithComplexPlugin(
//	    PluginForStruct(FormatSimpleStructField, FormatTupleStruct))
//
// See the repository README for a tuple STRUCT example. Application-specific presets
// (for example spanner-mycli table modes) should compose [FormatConfig] in the caller
// rather than adding new constructors here.
//
// [spanner-cli]: https://github.com/cloudspannerecosystem/spanner-cli
func SpannerCLICompatibleFormatConfig() *FormatConfig {
	return &FormatConfig{
		NullString: nullStringUpperCase,
		FormatComplexPlugins: []FormatComplexFunc{
			FormatSpannerCLIValue,
			PluginForArray(FormatUntypedArray),
			PluginForStruct(FormatSimpleStructField, FormatBracketStruct),
		},
	}
}

// FormatRowSpannerCLICompatible formats each column of row using [SpannerCLICompatibleFormatConfig].
func FormatRowSpannerCLICompatible(row *spanner.Row) ([]string, error) {
	return spannerCLICompatibleFormatConfig.FormatRow(row)
}

func FormatNullableSpannerCLICompatible(value NullableValue) (string, error) {
	// NULL is already handled before [PluginFromNullable] invokes the formatter; this
	// re-check keeps FormatNullableSpannerCLICompatible safe when used as a standalone callback.
	if value.IsNull() {
		return nullStringUpperCase, nil
	}

	switch v := value.(type) {
	case NullBytes:
		return base64.StdEncoding.EncodeToString(v), nil
	case spanner.NullFloat32:
		return formatSpannerCLIFloat(float64(v.Float32), 32), nil
	case spanner.NullFloat64:
		return formatSpannerCLIFloat(v.Float64, 64), nil
	case spanner.NullNumeric:
		return trimSpannerCLINumericFraction(v.String()), nil
	case spanner.PGNumeric:
		return trimSpannerCLINumericFraction(v.Numeric), nil
	case spanner.NullTime:
		return v.Time.Format(time.RFC3339Nano), nil
	// They are actually processed by the default case, but explicitly written here for clarity.
	case spanner.NullString, spanner.NullBool, spanner.NullInt64, spanner.NullDate,
		spanner.NullJSON, spanner.NullInterval, spanner.NullUUID, spanner.PGJsonB:
		return v.String(), nil
	default:
		return value.String(), nil
	}
}

// FormatColumnSpannerCLICompatible formats value using [SpannerCLICompatibleFormatConfig] at top level.
func FormatColumnSpannerCLICompatible(value spanner.GenericColumnValue) (string, error) {
	return spannerCLICompatibleFormatConfig.FormatToplevelColumn(value)
}

var (
	_ FormatArrayFunc       = FormatUntypedArray
	_ FormatStructParenFunc = FormatBracketStruct
	_ FormatStructFieldFunc = FormatSimpleStructField
)

func FormatBracketStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("[%v]", strings.Join(fieldStrings, ", ")), nil
}
