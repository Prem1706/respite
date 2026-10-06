.PHONY: build test run bench cluster cluster-down

build:
	go build -o respite ./cmd/respite

test:
	go vet ./...
	go test -race ./...

run: build
	./respite -metrics :9121

bench:
	bench/bench.sh

cluster:
	docker compose up -d --build

cluster-down:
	docker compose down
