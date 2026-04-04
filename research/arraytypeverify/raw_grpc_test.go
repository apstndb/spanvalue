// Raw [sppb.ExecuteSqlRequest] tests: set params without param_types (per API docs) and
// observe behavior when @p is an empty list (zero-length [structpb.ListValue]).
//
// See [sppb.ExecuteSqlRequest] fields params and param_types — param_types is optional and
// used when Cloud Spanner cannot infer the SQL type from the JSON-like params alone.
package arraytypeverify

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	spannergapic "cloud.google.com/go/spanner/apiv1"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanemuboost"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func grpcClientOptionsEmulator(endpoint string) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
}

func grpcClientOptionsReal() []option.ClientOption {
	return []option.ClientOption{}
}

// executeStreamingSQL runs ExecuteStreamingSql with the given request, returns the first
// non-nil ResultSetMetadata from PartialResultSets and any Recv error.
func firstMetadataFromExecuteStreamingSQL(
	ctx context.Context,
	cli *spannergapic.Client,
	req *sppb.ExecuteSqlRequest,
) (*sppb.ResultSetMetadata, error) {
	stream, err := cli.ExecuteStreamingSql(ctx, req)
	if err != nil {
		return nil, err
	}
	var md *sppb.ResultSetMetadata
	for {
		prs, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return md, err
		}
		if prs.GetMetadata() != nil {
			md = prs.GetMetadata()
		}
	}
	return md, nil
}

func formatMetadataShort(m *sppb.ResultSetMetadata) string {
	if m == nil {
		return "<nil>"
	}
	b, err := protojson.MarshalOptions{Indent: "  "}.Marshal(m)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// TestRawGRPC_EmptyListParamWithoutParamTypes uses the gapic Spanner client (gRPC) directly:
// params only, no param_types, @p bound to an empty list ([] in JSON terms).
func TestRawGRPC_EmptyListParamWithoutParamTypes_Emulator(t *testing.T) {
	env := spanemuboost.SetupEmulatorWithClients(t,
		spanemuboost.EnableInstanceAutoConfigOnly(),
		spanemuboost.WithRandomDatabaseID(),
	)
	ctx := context.Background()

	cli, err := spannergapic.NewClient(ctx, grpcClientOptionsEmulator(env.URI())...)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	sess, err := cli.CreateSession(ctx, &sppb.CreateSessionRequest{Database: env.DatabasePath()})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.DeleteSession(context.Background(), &sppb.DeleteSessionRequest{Name: sess.GetName()})
	})

	req := &sppb.ExecuteSqlRequest{
		Session: sess.GetName(),
		Sql:     `SELECT @p AS p`,
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"p": structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		// ParamTypes omitted: server must infer ARRAY element type from the value.
		QueryMode: sppb.ExecuteSqlRequest_PLAN,
	}

	md, streamErr := firstMetadataFromExecuteStreamingSQL(ctx, cli, req)
	t.Logf("empty list, no param_types (PLAN)\n  stream err: %v\n  first ResultSetMetadata:\n%s",
		streamErr, formatMetadataShort(md))
}

// TestRawGRPC_EmptyListWithExplicitArrayInt64ParamType is a control: same empty list_value in params,
// but param_types specifies ARRAY<INT64> so the request matches what gcvctor encodes with full Type metadata.
func TestRawGRPC_EmptyListWithExplicitArrayInt64ParamType_Emulator(t *testing.T) {
	env := spanemuboost.SetupEmulatorWithClients(t,
		spanemuboost.EnableInstanceAutoConfigOnly(),
		spanemuboost.WithRandomDatabaseID(),
	)
	ctx := context.Background()

	cli, err := spannergapic.NewClient(ctx, grpcClientOptionsEmulator(env.URI())...)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	sess, err := cli.CreateSession(ctx, &sppb.CreateSessionRequest{Database: env.DatabasePath()})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.DeleteSession(context.Background(), &sppb.DeleteSessionRequest{Name: sess.GetName()})
	})

	req := &sppb.ExecuteSqlRequest{
		Session: sess.GetName(),
		Sql:     `SELECT @p AS p`,
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"p": structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		ParamTypes: map[string]*sppb.Type{
			"p": {
				Code:             sppb.TypeCode_ARRAY,
				ArrayElementType: &sppb.Type{Code: sppb.TypeCode_INT64},
			},
		},
		QueryMode: sppb.ExecuteSqlRequest_PLAN,
	}

	md, streamErr := firstMetadataFromExecuteStreamingSQL(ctx, cli, req)
	t.Logf("empty list + param_types ARRAY<INT64> (PLAN)\n  stream err: %v\n  first ResultSetMetadata:\n%s",
		streamErr, formatMetadataShort(md))
}

// TestRawGRPC_Real repeats the undeclared-param-types empty-list case against a real instance.
func TestRawGRPC_EmptyListParamWithoutParamTypes_Real(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		t.Skip("unset SPANNER_EMULATOR_HOST")
	}
	project := os.Getenv("SPANNER_PROJECT_ID")
	instance := os.Getenv("SPANNER_INSTANCE_ID")
	database := os.Getenv("SPANNER_DATABASE_ID")
	if project == "" || instance == "" || database == "" {
		t.Skip("set SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID")
	}
	ctx := context.Background()
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	cli, err := spannergapic.NewClient(ctx, grpcClientOptionsReal()...)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	sess, err := cli.CreateSession(ctx, &sppb.CreateSessionRequest{Database: dbPath})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.DeleteSession(context.Background(), &sppb.DeleteSessionRequest{Name: sess.GetName()})
	})

	req := &sppb.ExecuteSqlRequest{
		Session: sess.GetName(),
		Sql:     `SELECT @p AS p`,
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"p": structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		QueryMode: sppb.ExecuteSqlRequest_PLAN,
	}

	md, streamErr := firstMetadataFromExecuteStreamingSQL(ctx, cli, req)
	t.Logf("empty list, no param_types (PLAN)\n  stream err: %v\n  first ResultSetMetadata:\n%s",
		streamErr, formatMetadataShort(md))
}
