TARGET := example
EXAMPLESRC := ./examples

all: fmt vet test

vet:
	go vet ./...

lint:
	golangci-lint run

fmt:
	gofmt -l -w .

test:
	go test -cover ./...

run-example:
	go run ${EXAMPLESRC}/...

generate:
	go get github.com/matryer/moq@latest
	go generate ./pkg/...

example: vet lint fmt
	go build -o ./bin/${TARGET} ${EXAMPLESRC}

clean:
	go clean ./...
	-rm -rf bin

.PHONY: build clean vet test
