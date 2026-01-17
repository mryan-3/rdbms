.PHONY: build test lint run web clean help

help:
	@echo "Available targets:"
	@echo "  build    - Build CLI binary"
	@echo "  test     - Run all tests"
	@echo "  lint     - Run golangci-lint"
	@echo "  run      - Run REPL"
	@echo "  web      - Run web app"
	@echo "  clean    - Clean build artifacts"

build:
	go build -o bin/rdbms cmd/rdbms/main.go

test:
	go test -v ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run

run: build
	./bin/rdbms

web:
	go run webapp/main.go

clean:
	rm -rf bin coverage.out coverage.html
