.PHONY: build test run bench

build:
	go build -o respite ./cmd/respite

test:
	go vet ./...
	go test -race ./...

run: build
	./respite

bench:
	bench/bench.sh
