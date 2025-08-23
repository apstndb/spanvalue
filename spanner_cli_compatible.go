package spanvalue

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

var SpannerCLICompatibleFormatConfig = FormatConfig{
	NullString:  nullStringUpperCase,
	FormatArray: FormatUntypedArray,
	FormatStruct: FormatStruct{
		FormatStructField: FormatSimpleStructField,
		FormatStructParen: FormatBracketStruct,
	},
	FormatNullable: FormatNullableSpannerCLICompatible,
}

func FormatRowSpannerCLICompatible(row *spanner.Row) ([]string, error) {
	return SpannerCLICompatibleFormatConfig.FormatRow(row)
}

var trailingPointZeroRe = regexp.MustCompile(`\.?0*$`)

func FormatNullableSpannerCLICompatible(value NullableValue) (string, error) {
	// Actually, it is redundant to check IsNull() here, but it is for consistency.
	if value.IsNull() {
		return nullStringUpperCase, nil
	}

	switch v := value.(type) {
	case NullBytes:
		return base64.StdEncoding.EncodeToString(v), nil
	case spanner.NullFloat32:
		return fmt.Sprintf("%f", v.Float32), nil
	case spanner.NullFloat64:
		return fmt.Sprintf("%f", v.Float64), nil
	case spanner.NullNumeric:
		return trailingPointZeroRe.ReplaceAllString(v.String(), ""), nil
	case spanner.NullTime:
		return v.Time.Format(time.RFC3339Nano), nil
	// They are actually processed by the default case, but explicitly written here for clarity.
	case spanner.NullString, spanner.NullBool, spanner.NullInt64, spanner.NullDate,
		spanner.NullJSON, spanner.NullInterval, spanner.NullUUID:
		return v.String(), nil
	default:
		return value.String(), nil
	}
}

func FormatColumnSpannerCLICompatible(value spanner.GenericColumnValue) (string, error) {
	return SpannerCLICompatibleFormatConfig.FormatToplevelColumn(value)
}

var (
	_ FormatArrayFunc       = FormatUntypedArray
	_ FormatStructParenFunc = FormatBracketStruct
	_ FormatStructFieldFunc = FormatSimpleStructField
)

func FormatBracketStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("[%v]", strings.Join(fieldStrings, ", "))
}
