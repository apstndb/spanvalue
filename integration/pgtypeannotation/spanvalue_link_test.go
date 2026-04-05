package pgtypeannotation_test

import (
	"testing"

	"github.com/apstndb/spanvalue"
)

func TestParentSpanvalueModuleLinked(t *testing.T) {
	t.Parallel()
	// Ensures go.mod replace (../..) resolves the repo-root module for this nested module.
	if spanvalue.ErrUnknownType.Error() == "" {
		t.Fatal("expected non-empty sentinel message")
	}
}
