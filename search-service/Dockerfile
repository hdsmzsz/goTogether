FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.work go.work
COPY pkg/ pkg/
COPY proto/ proto/
COPY search-service/ search-service/
RUN cd search-service && go mod download
RUN cd search-service && CGO_ENABLED=0 go build -o /search-service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /search-service /search-service
ENTRYPOINT ["/search-service"]
