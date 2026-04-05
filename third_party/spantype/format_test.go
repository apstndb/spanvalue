package spantype

import (
	"fmt"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	. "github.com/apstndb/spantype/typector"
)

func TestFormatType(t *testing.T) {
	for _, tt := range []struct {
		desc            string
		typ             *sppb.Type
		wantSimplest    string
		wantSimple      string
		wantNormal      string
		wantVerbose     string
		wantMoreVerbose string
	}{
		{
			desc:            "UNKNOWN",
			typ:             CodeToSimpleType(-1),
			wantSimplest:    "-1",
			wantSimple:      "UNKNOWN",
			wantNormal:      "UNKNOWN(-1)",
			wantVerbose:     "UNKNOWN(-1)",
			wantMoreVerbose: "UNKNOWN(-1)",
		},
		{
			desc:            "TYPE_CODE_UNSPECIFIED",
			typ:             CodeToSimpleType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED),
			wantSimplest:    "TYPE_CODE_UNSPECIFIED",
			wantSimple:      "TYPE_CODE_UNSPECIFIED",
			wantNormal:      "TYPE_CODE_UNSPECIFIED",
			wantVerbose:     "TYPE_CODE_UNSPECIFIED",
			wantMoreVerbose: "TYPE_CODE_UNSPECIFIED",
		},
		{
			desc:            "BOOL",
			typ:             CodeToSimpleType(sppb.TypeCode_BOOL),
			wantSimplest:    "BOOL",
			wantSimple:      "BOOL",
			wantNormal:      "BOOL",
			wantVerbose:     "BOOL",
			wantMoreVerbose: "BOOL",
		},
		{
			desc:            "INT64",
			typ:             CodeToSimpleType(sppb.TypeCode_INT64),
			wantSimplest:    "INT64",
			wantSimple:      "INT64",
			wantNormal:      "INT64",
			wantVerbose:     "INT64",
			wantMoreVerbose: "INT64",
		},
		{
			desc:            "FLOAT64",
			typ:             CodeToSimpleType(sppb.TypeCode_FLOAT64),
			wantSimplest:    "FLOAT64",
			wantSimple:      "FLOAT64",
			wantNormal:      "FLOAT64",
			wantVerbose:     "FLOAT64",
			wantMoreVerbose: "FLOAT64",
		},
		{
			desc:            "FLOAT32",
			typ:             CodeToSimpleType(sppb.TypeCode_FLOAT32),
			wantSimplest:    "FLOAT32",
			wantSimple:      "FLOAT32",
			wantNormal:      "FLOAT32",
			wantVerbose:     "FLOAT32",
			wantMoreVerbose: "FLOAT32",
		},
		{
			desc:            "TIMESTAMP",
			typ:             CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
			wantSimplest:    "TIMESTAMP",
			wantSimple:      "TIMESTAMP",
			wantNormal:      "TIMESTAMP",
			wantVerbose:     "TIMESTAMP",
			wantMoreVerbose: "TIMESTAMP",
		},
		{
			desc:            "DATE",
			typ:             CodeToSimpleType(sppb.TypeCode_DATE),
			wantSimplest:    "DATE",
			wantSimple:      "DATE",
			wantNormal:      "DATE",
			wantVerbose:     "DATE",
			wantMoreVerbose: "DATE",
		},
		{
			desc:            "STRING",
			typ:             CodeToSimpleType(sppb.TypeCode_STRING),
			wantSimplest:    "STRING",
			wantSimple:      "STRING",
			wantNormal:      "STRING",
			wantVerbose:     "STRING",
			wantMoreVerbose: "STRING",
		},
		{
			desc:            "BYTES",
			typ:             CodeToSimpleType(sppb.TypeCode_BYTES),
			wantSimplest:    "BYTES",
			wantSimple:      "BYTES",
			wantNormal:      "BYTES",
			wantVerbose:     "BYTES",
			wantMoreVerbose: "BYTES",
		},
		// ARRAY
		{
			desc:            "ARRAY",
			typ:             ElemCodeToArrayType(sppb.TypeCode_INT64),
			wantSimplest:    "ARRAY",
			wantSimple:      "ARRAY<INT64>",
			wantNormal:      "ARRAY<INT64>",
			wantVerbose:     "ARRAY<INT64>",
			wantMoreVerbose: "ARRAY<INT64>",
		},
		{
			desc:            "NUMERIC",
			typ:             CodeToSimpleType(sppb.TypeCode_NUMERIC),
			wantSimplest:    "NUMERIC",
			wantSimple:      "NUMERIC",
			wantNormal:      "NUMERIC",
			wantVerbose:     "NUMERIC",
			wantMoreVerbose: "NUMERIC",
		},
		{
			desc:            "JSON",
			typ:             CodeToSimpleType(sppb.TypeCode_JSON),
			wantSimplest:    "JSON",
			wantSimple:      "JSON",
			wantNormal:      "JSON",
			wantVerbose:     "JSON",
			wantMoreVerbose: "JSON",
		},
		{
			desc:            "INTERVAL",
			typ:             CodeToSimpleType(sppb.TypeCode_INTERVAL),
			wantSimplest:    "INTERVAL",
			wantSimple:      "INTERVAL",
			wantNormal:      "INTERVAL",
			wantVerbose:     "INTERVAL",
			wantMoreVerbose: "INTERVAL",
		},
		{
			desc:            "UUID",
			typ:             CodeToSimpleType(sppb.TypeCode_UUID),
			wantSimplest:    "UUID",
			wantSimple:      "UUID",
			wantNormal:      "UUID",
			wantVerbose:     "UUID",
			wantMoreVerbose: "UUID",
		},
		// STRUCT
		{
			desc:            "STRUCT with name",
			typ:             NameTypeToStructType("arr", ElemTypeToArrayType(NameCodeToStructType("n", sppb.TypeCode_INT64))),
			wantSimplest:    "STRUCT",
			wantSimple:      "STRUCT",
			wantNormal:      "STRUCT<ARRAY<STRUCT<INT64>>>",
			wantVerbose:     "STRUCT<arr ARRAY<STRUCT<n INT64>>>",
			wantMoreVerbose: "STRUCT<arr ARRAY<STRUCT<n INT64>>>",
		},
		{
			desc:            "STRUCT without name",
			typ:             NameTypeToStructType("", ElemTypeToArrayType(NameCodeToStructType("", sppb.TypeCode_INT64))),
			wantSimplest:    "STRUCT",
			wantSimple:      "STRUCT",
			wantNormal:      "STRUCT<ARRAY<STRUCT<INT64>>>",
			wantVerbose:     "STRUCT<ARRAY<STRUCT<INT64>>>",
			wantMoreVerbose: "STRUCT<ARRAY<STRUCT<INT64>>>",
		},
		// PROTO
		{
			desc:            "PROTO without package",
			typ:             FQNToProtoType("ProtoType"),
			wantSimplest:    "PROTO",
			wantSimple:      "ProtoType",
			wantNormal:      "ProtoType",
			wantVerbose:     "ProtoType",
			wantMoreVerbose: "PROTO<ProtoType>",
		},
		{
			desc:            "PROTO",
			typ:             FQNToProtoType("examples.ProtoType"),
			wantSimplest:    "PROTO",
			wantSimple:      "ProtoType",
			wantNormal:      "ProtoType",
			wantVerbose:     "examples.ProtoType",
			wantMoreVerbose: "PROTO<examples.ProtoType>",
		},
		// ENUM
		{
			desc:            "ENUM",
			typ:             FQNToEnumType("examples.EnumType"),
			wantSimplest:    "ENUM",
			wantSimple:      "EnumType",
			wantNormal:      "EnumType",
			wantVerbose:     "examples.EnumType",
			wantMoreVerbose: "ENUM<examples.EnumType>",
		},
		{
			desc:            "ENUM without package",
			typ:             FQNToEnumType("EnumType"),
			wantSimplest:    "ENUM",
			wantSimple:      "EnumType",
			wantNormal:      "EnumType",
			wantVerbose:     "EnumType",
			wantMoreVerbose: "ENUM<EnumType>",
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			if got := FormatTypeSimplest(tt.typ); tt.wantSimplest != got {
				t.Errorf("FormatTypeSimplest want: %v, got: %v", tt.wantSimplest, got)
			}
			if got := FormatTypeSimple(tt.typ); tt.wantSimple != got {
				t.Errorf("FormatTypeSimple want: %v, got: %v", tt.wantSimple, got)
			}
			if got := FormatTypeNormal(tt.typ); tt.wantNormal != got {
				t.Errorf("FormatTypeNormal want: %v, got: %v", tt.wantNormal, got)
			}
			if got := FormatTypeVerbose(tt.typ); tt.wantVerbose != got {
				t.Errorf("FormatTypeVerbose want: %v, got: %v", tt.wantVerbose, got)
			}
			if got := FormatTypeMoreVerbose(tt.typ); tt.wantMoreVerbose != got {
				t.Errorf("FormatTypeMoreVerbose want: %v, got: %v", tt.wantMoreVerbose, got)
			}
		})
	}
}

func TestFormatProtoEnum(t *testing.T) {
	tests := []struct {
		desc string
		typ  *sppb.Type
		want map[ProtoEnumMode]string
	}{
		{
			desc: "ENUM",
			typ:  FQNToEnumType("examples.EnumType"),
			want: map[ProtoEnumMode]string{
				ProtoEnumModeBase:         "ENUM",
				ProtoEnumModeLeaf:         "EnumType",
				ProtoEnumModeFull:         "examples.EnumType",
				ProtoEnumModeLeafWithKind: "ENUM<EnumType>",
				ProtoEnumModeFullWithKind: "ENUM<examples.EnumType>",
			},
		},
		{
			desc: "PROTO",
			typ:  FQNToProtoType("examples.ProtoType"),
			want: map[ProtoEnumMode]string{
				ProtoEnumModeBase:         "PROTO",
				ProtoEnumModeLeaf:         "ProtoType",
				ProtoEnumModeFull:         "examples.ProtoType",
				ProtoEnumModeLeafWithKind: "PROTO<ProtoType>",
				ProtoEnumModeFullWithKind: "PROTO<examples.ProtoType>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			for _, mode := range []ProtoEnumMode{ProtoEnumModeBase, ProtoEnumModeLeaf, ProtoEnumModeFull, ProtoEnumModeLeafWithKind, ProtoEnumModeFullWithKind} {
				t.Run(fmt.Sprint(mode), func(t *testing.T) {
					if got := FormatProtoEnum(tt.typ, mode); tt.want[mode] != got {
						t.Errorf("FormatProtoEnum want: %v, got: %v", tt.want[mode], got)
					}
				})

			}
		})
	}
}

func TestFormatType_PostgreSQLAnnotations(t *testing.T) {
	t.Parallel()

	if got, want := FormatTypeVerbose(PGNumeric()), "NUMERIC(PG_NUMERIC)"; got != want {
		t.Errorf("PGNumeric: got %q want %q", got, want)
	}
	if got, want := FormatTypeVerbose(PGJsonB()), "JSON(PG_JSONB)"; got != want {
		t.Errorf("PGJsonB: got %q want %q", got, want)
	}
	if got, want := FormatTypeVerbose(ElemTypeToArrayType(PGNumeric())), "ARRAY<NUMERIC(PG_NUMERIC)>"; got != want {
		t.Errorf("ARRAY<PG NUMERIC>: got %q want %q", got, want)
	}
}

func TestFormatTypeCode(t *testing.T) {
	tests := []struct {
		desc        string
		code        sppb.TypeCode
		want        string
		shouldPanic bool
		mode        UnknownMode
	}{
		{
			desc:        "UNKNOWN should panic",
			code:        -1,
			shouldPanic: true,
			mode:        UnknownModePanic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			defer func() {
				if rec := recover(); rec != nil && !tt.shouldPanic {
					t.Errorf("FormatTypeCode should not panic: %v", rec)
				}
			}()
			if got := FormatTypeCode(tt.code, tt.mode); tt.want != got {
				t.Errorf("FormatTypeCode want: %v, got: %v", tt.want, got)
			}
			if tt.shouldPanic {
				t.Errorf("FormatTypeCode should panic")
			}
		})
	}
}
