.PHONY: build check fmt fmt-check lint test test-integration test-v vet

build:
	go build ./...

check: fmt-check vet build test lint

fmt:
	go fmt ./...

fmt-check:
	@files="$$(git ls-files -z -- '*.go' | xargs -0 sh -c 'if [ "$$#" -eq 0 ]; then exit 0; fi; gofmt -l "$$@"' sh)"; \
	if [ -n "$$files" ]; then \
		echo "$$files"; \
		exit 1; \
	fi

lint:
	golangci-lint run

test:
	go test ./...

# Nested module: PostgreSQL TypeAnnotation probes (Docker or real Spanner; see integration/pgtypeannotation/README.md).
test-integration:
	cd integration/pgtypeannotation && go test -count=1 ./...

test-v:
	go test -v ./...

vet:
	go vet ./...
