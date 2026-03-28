.PHONY: build check fmt fmt-check lint test test-v vet

build:
	go build ./...

check: fmt-check vet build test lint

fmt:
	go fmt ./...

fmt-check:
	@files="$$(gofmt -l $$(git ls-files '*.go'))"; \
	if [ -n "$$files" ]; then \
		echo "$$files"; \
		exit 1; \
	fi

lint:
	golangci-lint run

test:
	go test ./...

test-v:
	go test -v ./...

vet:
	go vet ./...
