// Integration tests for PostgreSQL-dialect Spanner: TypeAnnotation on query parameters
// and ResultSetMetadata.row_type when reading rows.
//
// Real instance: set SPANVALUE_PROJECT_ID and SPANVALUE_INSTANCE_ID (Application Default Credentials).
//
// Default (no env): starts the Cloud Spanner emulator via [github.com/apstndb/spanemuboost]
// (Docker / testcontainers) with POSTGRESQL dialect.
//
// Manual emulator: set SPANNER_EMULATOR_HOST (e.g. localhost:9010). Creates project/instance/database
// on the emulator; if CreateDatabase with POSTGRESQL dialect fails, the test is skipped.
package pgtypeannotation_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanemuboost"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	emulatorProjectID   = "test-project"
	emulatorInstanceID  = "test-instance"
	emulatorInstanceCfg = "emulator-config"
)

func clientOpts() []option.ClientOption {
	if h := os.Getenv("SPANNER_EMULATOR_HOST"); h != "" {
		return []option.ClientOption{
			option.WithEndpoint(h),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		}
	}
	return nil
}

func projectID(t *testing.T) string {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		return emulatorProjectID
	}
	p := os.Getenv("SPANVALUE_PROJECT_ID")
	if p == "" {
		t.Fatal("SPANVALUE_PROJECT_ID is not set")
	}
	return p
}

func instanceID(t *testing.T) string {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		return emulatorInstanceID
	}
	inst := os.Getenv("SPANVALUE_INSTANCE_ID")
	if inst == "" {
		t.Fatal("SPANVALUE_INSTANCE_ID is not set")
	}
	return inst
}

func ensureEmulatorInstance(ctx context.Context, t *testing.T) {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		return
	}
	instAdmin, err := instance.NewInstanceAdminClient(ctx, clientOpts()...)
	if err != nil {
		t.Fatalf("NewInstanceAdminClient: %v", err)
	}
	defer instAdmin.Close()

	parent := fmt.Sprintf("projects/%s", emulatorProjectID)
	name := fmt.Sprintf("%s/instances/%s", parent, emulatorInstanceID)
	_, err = instAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: name})
	if err == nil {
		return
	}

	op, err := instAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     parent,
		InstanceId: emulatorInstanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", emulatorProjectID, emulatorInstanceCfg),
			DisplayName: emulatorInstanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("CreateInstance Wait: %v", err)
	}
}

func createPostgreSQLDatabase(ctx context.Context, t *testing.T, dbID string) (dbPath string, drop func(), err error) {
	t.Helper()
	project := projectID(t)
	inst := instanceID(t)
	parent := fmt.Sprintf("projects/%s/instances/%s", project, inst)

	dbAdmin, err := database.NewDatabaseAdminClient(ctx, clientOpts()...)
	if err != nil {
		return "", nil, err
	}

	// PostgreSQL dialect: quoted database name in CREATE DATABASE (see go-sql-spanner emulator sample).
	createStatement := fmt.Sprintf(`CREATE DATABASE "%s"`, dbID)
	op, err := dbAdmin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: createStatement,
		DatabaseDialect: adminpb.DatabaseDialect_POSTGRESQL,
	})
	if err != nil {
		dbAdmin.Close()
		return "", nil, err
	}
	if _, err := op.Wait(ctx); err != nil {
		dbAdmin.Close()
		return "", nil, err
	}

	dbPath = fmt.Sprintf("%s/databases/%s", parent, dbID)
	return dbPath, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = dbAdmin.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: dbPath})
		_ = dbAdmin.Close()
	}, nil
}

func TestPostgreSQL_TypeAnnotation_QueryParam_and_RowType(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	var client *spanner.Client
	cleanup := func() {}

	pid := os.Getenv("SPANVALUE_PROJECT_ID")
	iid := os.Getenv("SPANVALUE_INSTANCE_ID")
	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")

	switch {
	case pid != "" && iid != "":
		dbID := fmt.Sprintf("pgta_%d", time.Now().UnixNano())
		dbPath, dropDB, err := createPostgreSQLDatabase(ctx, t, dbID)
		if err != nil {
			t.Fatalf("CreateDatabase: %v", err)
		}
		c, err := spanner.NewClient(ctx, dbPath, clientOpts()...)
		if err != nil {
			dropDB()
			t.Fatalf("NewClient: %v", err)
		}
		client = c
		cleanup = func() {
			c.Close()
			dropDB()
		}

	case pid != "" || iid != "":
		t.Skip("set both SPANVALUE_PROJECT_ID and SPANVALUE_INSTANCE_ID for real Cloud Spanner, or neither")

	case emulatorHost != "":
		ensureEmulatorInstance(ctx, t)
		dbID := fmt.Sprintf("pgta_%d", time.Now().UnixNano())
		dbPath, dropDB, err := createPostgreSQLDatabase(ctx, t, dbID)
		if err != nil {
			t.Skipf("PostgreSQL dialect database not available on this environment: %v", err)
		}
		c, err := spanner.NewClient(ctx, dbPath, clientOpts()...)
		if err != nil {
			dropDB()
			t.Fatalf("NewClient: %v", err)
		}
		client = c
		cleanup = func() {
			c.Close()
			dropDB()
		}

	default:
		env := spanemuboost.SetupEmulatorWithClients(t,
			spanemuboost.WithDatabaseDialect(adminpb.DatabaseDialect_POSTGRESQL),
			spanemuboost.WithRandomDatabaseID(),
		)
		client = env.Client
	}

	defer cleanup()

	t.Run("PGNumeric_param_and_row_metadata", func(t *testing.T) {
		// PostgreSQL dialect uses $1, $2, … placeholders; params map keys are still p1, p2, …
		// (see cloud.google.com/go/spanner integration tests).
		stmt := spanner.Statement{
			SQL: `SELECT $1 AS out_col`,
			Params: map[string]interface{}{
				"p1": spanner.PGNumeric{Numeric: "3.14", Valid: true},
			},
		}
		iter := client.Single().Query(ctx, stmt)
		defer iter.Stop()

		row, err := iter.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if iter.Metadata == nil || iter.Metadata.RowType == nil {
			t.Fatal("expected ResultSetMetadata.RowType after first Next")
		}
		fields := iter.Metadata.RowType.GetFields()
		if len(fields) != 1 {
			t.Fatalf("fields: got %d want 1", len(fields))
		}
		typ := fields[0].GetType()
		if typ.GetCode() != sppb.TypeCode_NUMERIC {
			t.Errorf("column type code: got %v want NUMERIC", typ.GetCode())
		}
		if typ.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_NUMERIC {
			t.Errorf("column TypeAnnotation: got %v want PG_NUMERIC", typ.GetTypeAnnotation())
		}

		var got spanner.PGNumeric
		if err := row.Columns(&got); err != nil {
			t.Fatalf("Columns: %v", err)
		}
		want := spanner.PGNumeric{Numeric: "3.14", Valid: true}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("value (-want +got):\n%s", diff)
		}

		if _, err := iter.Next(); err != iterator.Done {
			t.Fatalf("expected single row, got err=%v", err)
		}
	})

	t.Run("PGJsonB_param_and_row_metadata", func(t *testing.T) {
		stmt := spanner.Statement{
			SQL: `SELECT $1 AS out_col`,
			Params: map[string]interface{}{
				"p1": spanner.PGJsonB{Value: map[string]any{"k": 1.0}, Valid: true},
			},
		}
		iter := client.Single().Query(ctx, stmt)
		defer iter.Stop()

		row, err := iter.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		fields := iter.Metadata.RowType.GetFields()
		if len(fields) != 1 {
			t.Fatalf("fields: got %d want 1", len(fields))
		}
		typ := fields[0].GetType()
		if typ.GetCode() != sppb.TypeCode_JSON {
			t.Errorf("column type code: got %v want JSON", typ.GetCode())
		}
		if typ.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_JSONB {
			t.Errorf("column TypeAnnotation: got %v want PG_JSONB", typ.GetTypeAnnotation())
		}

		var got spanner.PGJsonB
		if err := row.Columns(&got); err != nil {
			t.Fatalf("Columns: %v", err)
		}
		if !got.Valid {
			t.Fatal("expected valid PGJsonB")
		}

		if _, err := iter.Next(); err != iterator.Done {
			t.Fatalf("expected single row, got err=%v", err)
		}
	})
}
