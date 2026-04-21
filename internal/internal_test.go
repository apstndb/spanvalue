package internal

import (
	"errors"
	"testing"
)

func TestAssembleResolvedJSONObject_MismatchedCounts(t *testing.T) {
	t.Parallel()

	_, err := AssembleResolvedJSONObject([]string{"a"}, []string{"1", "2"})
	if !errors.Is(err, ErrMismatchedJSONObjectFields) {
		t.Fatalf("error = %v, want ErrMismatchedJSONObjectFields", err)
	}
}

func TestAssembleJSONObjectWithMarshaledKeys_MismatchedCounts(t *testing.T) {
	t.Parallel()

	_, err := AssembleJSONObjectWithMarshaledKeys([][]byte{[]byte(`"a"`), []byte(`"b"`)}, []string{"1"})
	if !errors.Is(err, ErrMismatchedJSONObjectFields) {
		t.Fatalf("error = %v, want ErrMismatchedJSONObjectFields", err)
	}
}
