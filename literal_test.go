//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// this file is copied from spanner-dump to ensure compatibility
// https://github.com/cloudspannerecosystem/spanner-dump/blob/6983541f4cffd4f032e4577efdf27222f3a5df99/decoder_test.go

package spanvalue

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spantype/typector"
	"github.com/samber/lo"

	"github.com/apstndb/spanvalue/gcvctor"
)

func mustParseTimeString(t *testing.T, timeStr string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		t.Fatalf("time.Parse unexpectedly failed: %v", err)
	}
	return parsed
}

func TestDecodeColumnLiteral(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		value interface{}
		want  string
	}{
		// non-nullable
		{
			desc:  "true",
			value: true,
			want:  "true",
		},
		{
			desc:  "false",
			value: false,
			want:  "false",
		},
		{
			desc:  "bytes",
			value: []byte("\x61\x62\x63\x01\xa0"),
			want:  `b"abc\x01\xa0"`,
		},
		{
			desc:  "float64",
			value: 1.23,
			want:  "1.23",
		},
		{
			desc:  "math.MaxFloat64",
			value: math.MaxFloat64,
			want:  "1.7976931348623157e+308",
		},
		{
			desc:  "-math.MaxFloat64",
			value: -math.MaxFloat64,
			want:  "-1.7976931348623157e+308",
		},
		{
			desc:  "math.SmallestNonzeroFloat64",
			value: math.SmallestNonzeroFloat64,
			want:  "5e-324",
		},
		{
			desc:  "-math.SmallestNonzeroFloat64",
			value: -math.SmallestNonzeroFloat64,
			want:  "-5e-324",
		},
		{
			desc:  "NaN",
			value: math.NaN(),
			want:  "CAST('nan' AS FLOAT64)",
		},
		{
			desc:  "Inf",
			value: math.Inf(+1),
			want:  "CAST('inf' AS FLOAT64)",
		},
		{
			desc:  "-Inf",
			value: math.Inf(-1),
			want:  "CAST('-inf' AS FLOAT64)",
		},
		{
			desc:  "int64",
			value: 123,
			want:  "123",
		},
		{
			desc:  "string",
			value: "foo",
			want:  `"foo"`,
		},
		{
			desc:  "string with double-quote",
			value: `foo"bar`,
			want:  `"foo\"bar"`,
		},
		{
			desc:  "string with new line",
			value: "foo\nbar",
			want:  `"foo\nbar"`,
		},
		{
			desc:  "timestamp",
			value: time.Unix(1516676400, 0),
			want:  `TIMESTAMP "2018-01-23T03:00:00Z"`,
		},
		{
			desc:  "date",
			value: civil.DateOf(mustParseTimeString(t, "2018-01-23T05:00:00+09:00")),
			want:  `DATE "2018-01-23"`,
		},
		{
			desc:  "numeric",
			value: big.NewRat(1234123456789, 1e9),
			want:  `NUMERIC "1234.123456789"`,
		},
		{
			desc:  "numeric with minimum value",
			value: mustBigRatFromString("-99999999999999999999999999999.999999999"),
			want:  `NUMERIC "-99999999999999999999999999999.999999999"`,
		},
		{
			desc:  "numeric with maximum value",
			value: mustBigRatFromString("99999999999999999999999999999.999999999"),
			want:  `NUMERIC "99999999999999999999999999999.999999999"`,
		},
		{
			desc:  "json",
			value: spanner.NullJSON{Value: jsonMessage{Msg: "foo"}, Valid: true},
			want:  `JSON "{\"msg\":\"foo\"}"`,
		},
		{
			desc:  "json with null",
			value: spanner.NullJSON{Value: nil, Valid: true},
			want:  `JSON "null"`,
		},
		{
			desc:  "json with nested double-quoted string",
			value: spanner.NullJSON{Value: jsonMessage{Msg: "\"foo\""}, Valid: true},
			want:  `JSON "{\"msg\":\"\\\"foo\\\"\"}"`,
		},
		{
			desc: "interval",
			value: spanner.NullInterval{Interval: spanner.Interval{
				Months: 13,
				Days:   1,
				Nanos:  big.NewInt((3600 + 60 + 1) * 1000 * 1000 * 1000),
			}, Valid: true},
			want: `CAST("P1Y1M1DT1H1M1S" AS INTERVAL)`,
		},

		// nullable
		{
			desc:  "null bool",
			value: spanner.NullBool{Bool: false, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null bytes",
			value: []byte(nil),
			want:  "NULL",
		},
		{
			desc:  "null float64",
			value: spanner.NullFloat64{Float64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null int64",
			value: spanner.NullInt64{Int64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null string",
			value: spanner.NullString{StringVal: "", Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null time",
			value: spanner.NullTime{Time: time.Unix(0, 0), Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null date",
			value: spanner.NullDate{Date: civil.DateOf(time.Unix(0, 0)), Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null numeric",
			value: spanner.NullNumeric{Numeric: big.Rat{}, Valid: false},
			want:  `NULL`,
		},
		{
			desc:  "null json",
			value: spanner.NullJSON{Value: jsonMessage{}, Valid: false},
			want:  `NULL`,
		},
		{
			desc:  "null interval",
			value: spanner.NullInterval{Valid: false},
			want:  `NULL`,
		},
		{
			desc:  "proto",
			value: gcvctor.ProtoValue("package.ProtoType", []byte("deadbeef")),
			want:  "CAST(b\"deadbeef\" AS `package.ProtoType`)",
		},
		{
			desc:  "enum",
			value: gcvctor.EnumValue("package.EnumType", 42),
			want:  "CAST(42 AS `package.EnumType`)",
		},
		{
			desc:  "null proto",
			value: gcvctor.TypedNull(typector.FQNToProtoType("package.ProtoType")),
			want:  "NULL",
		},
		{
			desc:  "null enum",
			value: gcvctor.TypedNull(typector.FQNToEnumType("package.EnumType")),
			want:  "NULL",
		},
		// array non-nullable
		{
			desc:  "empty array",
			value: []bool{},
			want:  "[]",
		},
		{
			desc:  "array bool",
			value: []bool{true, false},
			want:  "[true, false]",
		},
		{
			desc:  "array bytes",
			value: [][]byte{{'"', 'a', 'b', 'c', '"'}, {'\'', 'd', 'e', 'f', '\''}},
			want:  `[b'"abc"', b"'def'"]`,
		},
		{
			desc:  "array float64",
			value: []float64{1.23, 2.45},
			want:  "[1.23, 2.45]",
		},
		{
			desc:  "array int64",
			value: []int64{123, 456},
			want:  "[123, 456]",
		},
		{
			desc:  "array string",
			value: []string{"foo", "bar"},
			want:  `["foo", "bar"]`,
		},
		{
			desc:  "array timestamp",
			value: []time.Time{time.Unix(1516676400, 0), time.Unix(1516680000, 0)},
			want:  `[TIMESTAMP "2018-01-23T03:00:00Z", TIMESTAMP "2018-01-23T04:00:00Z"]`,
		},
		{
			desc:  "array date",
			value: []civil.Date{civil.DateOf(mustParseTimeString(t, "2018-01-23T05:00:00+09:00")), civil.DateOf(mustParseTimeString(t, "2018-01-24T05:00:00+09:00"))},
			want:  `[DATE "2018-01-23", DATE "2018-01-24"]`,
		},
		{
			desc:  "array numeric",
			value: []*big.Rat{big.NewRat(1234123456789, 1e9), big.NewRat(123456789, 1e5)},
			want:  `[NUMERIC "1234.123456789", NUMERIC "1234.567890000"]`,
		},
		{
			desc: "array json",
			value: []spanner.NullJSON{
				{Value: jsonMessage{Msg: "foo"}, Valid: true},
				{Value: jsonMessage{Msg: "bar"}, Valid: true},
			},
			want: `[JSON "{\"msg\":\"foo\"}", JSON "{\"msg\":\"bar\"}"]`,
		},
		{
			desc: "array interval",
			value: []spanner.NullInterval{
				{Interval: spanner.Interval{Months: 1}, Valid: true},
				{Interval: spanner.Interval{Days: 1}, Valid: true},
			},
			want: `[CAST("P1M" AS INTERVAL), CAST("P1D" AS INTERVAL)]`,
		},
		{
			desc:  "array proto",
			value: lo.Must(gcvctor.ArrayValue(gcvctor.ProtoValue("package.ProtoType", []byte("deadbeef")))),
			want:  "[CAST(b\"deadbeef\" AS `package.ProtoType`)]",
		},
		{
			desc:  "array enum",
			value: lo.Must(gcvctor.ArrayValue(gcvctor.EnumValue("package.EnumType", 42))),
			want:  "[CAST(42 AS `package.EnumType`)]",
		},

		// array nullable
		{
			desc:  "null array bool",
			value: []bool(nil),
			want:  "NULL",
		},
		{
			desc:  "null array bytes",
			value: [][]byte(nil),
			want:  "NULL",
		},
		{
			desc:  "nul array float64",
			value: []float64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array int64",
			value: []int64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array string",
			value: []string(nil),
			want:  "NULL",
		},
		{
			desc:  "null array timestamp",
			value: []time.Time(nil),
			want:  "NULL",
		},
		{
			desc:  "null array date",
			value: []civil.Date(nil),
			want:  "NULL",
		},
		{
			desc:  "null array numeric",
			value: []*big.Rat(nil),
			want:  "NULL",
		},
		{
			desc:  "null array json",
			value: []spanner.NullJSON(nil),
			want:  "NULL",
		},
		{
			desc:  "null array interval",
			value: []spanner.NullInterval(nil),
			want:  "NULL",
		},
		{
			desc:  "null array proto",
			value: gcvctor.TypedNull(typector.FQNToProtoType("package.ProtoType")),
			want:  "NULL",
		},
		{
			desc:  "null array enum",
			value: gcvctor.TypedNull(typector.FQNToEnumType("package.EnumType")),
			want:  "NULL",
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got, err := FormatColumnLiteral(createColumnValue(t, tt.value))
			if err != nil {
				t.Error(err)
			}
			if got != tt.want {
				t.Errorf("DecodeColumn(%v) = %q, want = %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestDecodeColumn_roundtripFloat64(t *testing.T) {
	for _, tt := range []float64{
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		-math.SmallestNonzeroFloat64,
	} {
		s, err := FormatColumnLiteral(createColumnValue(t, spanner.NullFloat64{Valid: true, Float64: tt}))
		if err != nil {
			t.Error(err)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Error(err)
		}
		if f != tt {
			t.Errorf("expected: %g, actual: %g\n", tt, f)
		}
	}
}

func TestDecodeRowLiteral(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		values []interface{}
		want   []string
	}{
		{
			desc:   "non-null columns",
			values: []interface{}{"foo", 123},
			want:   []string{`"foo"`, "123"},
		},
		{
			desc:   "non-null column and null column",
			values: []interface{}{"foo", spanner.NullString{StringVal: "", Valid: false}},
			want:   []string{`"foo"`, "NULL"},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got, err := FormatRowLiteral(createRow(t, tt.values))
			if err != nil {
				t.Error(err)
			}
			if !equalStringSlice(got, tt.want) {
				t.Errorf("DecodeRow(%v) = %v, want = %v", tt.values, got, tt.want)
			}
		})
	}
}

func mustBigRatFromString(s string) *big.Rat {
	r := &big.Rat{}
	r, ok := r.SetString(s)
	if !ok {
		panic(fmt.Sprintf("invalid string for big.Rat: %q", s))
	}
	return r
}
