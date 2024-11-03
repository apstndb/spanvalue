package spanvalue

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	if value.IsNull() {
		return nullStringUpperCase, nil
	}

	switch v := value.(type) {
	case spanner.NullString:
		return v.StringVal, nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case NullBytes:
		return base64.StdEncoding.EncodeToString(v), nil
	case spanner.NullFloat32:
		return fmt.Sprintf("%f", v.Float32), nil
	case spanner.NullFloat64:
		return fmt.Sprintf("%f", v.Float64), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return trailingPointZeroRe.ReplaceAllString(v.Numeric.FloatString(spanner.NumericScaleDigits), ""), nil
	case spanner.NullTime:
		return v.Time.Format(time.RFC3339Nano), nil
	case spanner.NullDate:
		return strings.Trim(v.String(), `"`), nil
	case spanner.NullJSON:
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

func FormatBracketStruct(typ *spannerpb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("[%v]", strings.Join(fieldStrings, ", "))
}
