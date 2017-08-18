all: test testrace

deps:
	go get -d -v google.golang.org/grpc/...

updatedeps:
	go get -d -v -u -f google.golang.org/grpc/...

testdeps:
	go get -d -v -t google.golang.org/grpc/...

updatetestdeps:
	go get -d -v -t -u -f google.golang.org/grpc/...

build: ## Compile packages.
build: deps
	go build google.golang.org/grpc/...

proto: ## Regenerated generated code for protobuf definitions.
proto:
	@ if ! which protoc > /dev/null; then \
		echo "error: protoc not installed" >&2; \
		exit 1; \
	fi
	go get -u -v github.com/golang/protobuf/protoc-gen-go
	# use $$dir as the root for all proto files in the same directory
	for dir in $$(git ls-files '*.proto' | xargs -n1 dirname | uniq); do \
		protoc -I $$dir --go_out=plugins=grpc:$$dir $$dir/*.proto; \
	done

test: ## Run tests.
test: testdeps
	go test -v -cpu 1,4 google.golang.org/grpc/...

testrace: ## Run tests with the Go race detector enabled.
testrace: testdeps
	go test -v -race -cpu 1,4 google.golang.org/grpc/...

clean: ## Clean all auto-generated artifacts.
clean:
	go clean -i google.golang.org/grpc/...

coverage: ## Run coverage test.
coverage: testdeps
	./coverage.sh --coveralls

help: ## Print help for targets with comments.
	@echo "Usage:"
	@echo "  make [target...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(shell tput setaf 6 2>/dev/null)%-30s$(shell tput sgr0 2>/dev/null) %s\n", $$1, $$2}'

.PHONY: all deps updatedeps testdeps updatetestdeps build proto test testrace \
	clean coverage
