.PHONY: build check fmt fmt-check lint test test-v vet vulncheck

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

test-v:
	go test -v ./...

vet:
	go vet ./...

# Mirrors the CI govulncheck job for local parity. Deliberately not part of
# `check`: it needs network access (vuln DB + @latest scanner) and would slow
# the inner loop.
vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
