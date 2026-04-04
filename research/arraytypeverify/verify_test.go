// Package arraytypeverify holds a disposable research harness (tests). It exercises Spanner with
// spanner.GenericColumnValue whose Type metadata may be incomplete (e.g. ARRAY without
// array_element_type). Run against the emulator (Docker) or a real database; see README.md.
package arraytypeverify

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanemuboost"
	"github.com/apstndb/spanvalue/gcvctor"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

type caseSpec struct {
	name string
	gcv  spanner.GenericColumnValue
}

func allCases(t *testing.T) []caseSpec {
	t.Helper()

	emptyInt64Array, err := gcvctor.ArrayValue()
	if err != nil {
		t.Fatalf("ArrayValue(): %v", err)
	}
	elemInt64 := int64ElemType()
	emptyTyped, err := gcvctor.ArrayValueWithType(elemInt64)
	if err != nil {
		t.Fatalf("ArrayValueWithType: %v", err)
	}

	return []caseSpec{
		{
			name: "wellformed_ArrayValue_empty_defaults_to_ARRAY_INT64",
			gcv:  emptyInt64Array,
		},
		{
			name: "wellformed_ArrayValueWithType_empty_INT64",
			gcv:  emptyTyped,
		},
		{
			name: "malformed_ARRAY_missing_array_element_type",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code: sppb.TypeCode_ARRAY,
				},
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
	}
}

func int64ElemType() *sppb.Type {
	return gcvctor.Int64Value(0).Type
}

func statementForCase(c caseSpec) spanner.Statement {
	return spanner.Statement{
		SQL: `SELECT @p AS p`,
		Params: map[string]interface{}{
			"p": c.gcv,
		},
	}
}

// tryNormalQuery runs ExecuteSql in NORMAL mode (result rows).
func tryNormalQuery(ctx context.Context, client *spanner.Client, stmt spanner.Statement) error {
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	return err
}

// tryQueryWithPlanMetadata runs ExecuteSql with QueryMode PLAN and returns the final
// RowIterator.Metadata (see [sppb.ResultSetMetadata], including undeclared_parameters for
// bound params) and any error from draining the stream. QueryPlan may be set on the iterator
// when the backend attaches plan nodes to stats.
func tryQueryWithPlanMetadata(ctx context.Context, client *spanner.Client, stmt spanner.Statement) (
	md *sppb.ResultSetMetadata,
	qp *sppb.QueryPlan,
	planErr error,
) {
	mode := sppb.ExecuteSqlRequest_PLAN
	iter := client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Mode: &mode})
	defer iter.Stop()

	var streamErr error
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			streamErr = err
			break
		}
	}
	return iter.Metadata, iter.QueryPlan, streamErr
}

func formatMetadata(m *sppb.ResultSetMetadata) string {
	if m == nil {
		return "<nil>"
	}
	b, err := protojson.MarshalOptions{Indent: "  "}.Marshal(m)
	if err != nil {
		return fmt.Sprintf("<marshal: %v>", err)
	}
	return string(b)
}

func logOutcome(
	t *testing.T,
	backend string,
	c caseSpec,
	queryErr error,
	md *sppb.ResultSetMetadata,
	qp *sppb.QueryPlan,
	planErr error,
) {
	t.Helper()

	qStr := "OK"
	if queryErr != nil {
		qStr = queryErr.Error()
	}
	planStr := "OK"
	if planErr != nil {
		planStr = planErr.Error()
	}
	qpNote := "QueryPlan=<nil>"
	if qp != nil {
		qpNote = fmt.Sprintf("QueryPlan present (plan_nodes=%d)", len(qp.GetPlanNodes()))
	}

	t.Logf("[%s] %s\n  Query (NORMAL): %s\n  QueryWithOptions (PLAN) stream: %s\n  %s\n  ResultSetMetadata:\n%s",
		backend, c.name, qStr, planStr, qpNote, formatMetadata(md))
}

// TestAgainstEmulator uses testcontainers (Docker). Skips if Docker is unavailable.
func TestAgainstEmulator(t *testing.T) {
	env := spanemuboost.SetupEmulatorWithClients(t,
		spanemuboost.EnableInstanceAutoConfigOnly(),
		spanemuboost.WithRandomDatabaseID(),
	)
	ctx := context.Background()
	client := env.Client

	for _, c := range allCases(t) {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stmt := statementForCase(c)
			qErr := tryNormalQuery(ctx, client, stmt)
			md, qp, pErr := tryQueryWithPlanMetadata(ctx, client, stmt)
			logOutcome(t, "emulator", c, qErr, md, qp, pErr)
		})
	}
}

// TestAgainstReal runs only when SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID are set
// and SPANNER_EMULATOR_HOST is unset. Uses application default credentials.
func TestAgainstReal(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		t.Skip("unset SPANNER_EMULATOR_HOST to run against a real instance")
	}
	project := os.Getenv("SPANNER_PROJECT_ID")
	instance := os.Getenv("SPANNER_INSTANCE_ID")
	database := os.Getenv("SPANNER_DATABASE_ID")
	if project == "" || instance == "" || database == "" {
		t.Skip("set SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID")
	}

	ctx := context.Background()
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	for _, c := range allCases(t) {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stmt := statementForCase(c)
			qErr := tryNormalQuery(ctx, client, stmt)
			md, qp, pErr := tryQueryWithPlanMetadata(ctx, client, stmt)
			logOutcome(t, "real", c, qErr, md, qp, pErr)
		})
	}
}

// TestSummarizeEnv prints effective env (no secrets) for debugging CI logs.
func TestSummarizeEnv(t *testing.T) {
	t.Log("SPANNER_EMULATOR_HOST=", os.Getenv("SPANNER_EMULATOR_HOST"))
	t.Log("SPANNER_PROJECT_ID=", os.Getenv("SPANNER_PROJECT_ID"))
	t.Log("SPANNER_INSTANCE_ID=", os.Getenv("SPANNER_INSTANCE_ID"))
	t.Log("SPANNER_DATABASE_ID=", os.Getenv("SPANNER_DATABASE_ID"))
}
