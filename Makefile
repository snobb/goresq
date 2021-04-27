TARGET := example
EXAMPLESRC := ./examples

all: fmt vet test

vet:
	go vet ./...

fmt:
	gofmt -l -w .

test:
	go test -cover ./...

run:
	go run ./examples/...

examples: vet fmt
	go build -o ./bin/${TARGET} ${EXAMPLESRC}

clean:
	go clean ./...
	-rm -rf bin

.PHONY: build clean vet test
