TARGET := example
EXAMPLESRC := ./examples

all: fmt vet test

vet:
	go vet ./...

fmt:
	gofmt -l -w .

test: generate
	go test -cover ./...

run: generate
	go run ./examples/...

generate:
	go generate ./pkg/...

examples: generate vet fmt
	go build -o ./bin/${TARGET} ${EXAMPLESRC}

clean:
	go clean ./...
	-rm -rf pkg/*/mock
	-rm -rf bin

.PHONY: build clean vet test
