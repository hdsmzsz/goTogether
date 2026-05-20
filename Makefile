.PHONY: proto build run-all test docker-up docker-down clean

proto:
	protoc --go_out=. --go-grpc_out=. proto/user/user.proto
	protoc --go_out=. --go-grpc_out=. proto/doc/doc.proto
	protoc --go_out=. --go-grpc_out=. proto/collab/collab.proto
	protoc --go_out=. --go-grpc_out=. proto/search/search.proto

build:
	cd gateway && go build -o ../bin/gateway .
	cd user-service && go build -o ../bin/user-service .
	cd doc-service && go build -o ../bin/doc-service .
	cd collab-service && go build -o ../bin/collab-service .
	cd search-service && go build -o ../bin/search-service .

tidy:
	cd pkg && go mod tidy
	cd proto && go mod tidy
	cd gateway && go mod tidy
	cd user-service && go mod tidy
	cd doc-service && go mod tidy
	cd collab-service && go mod tidy
	cd search-service && go mod tidy

test:
	cd user-service && go test ./...
	cd doc-service && go test ./...
	cd collab-service && go test ./...
	cd search-service && go test ./...

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

clean:
	rm -rf bin/
	docker compose down -v
