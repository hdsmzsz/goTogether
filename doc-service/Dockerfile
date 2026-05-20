FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.work go.work
COPY pkg/ pkg/
COPY proto/ proto/
COPY doc-service/ doc-service/
RUN cd doc-service && go mod download
RUN cd doc-service && CGO_ENABLED=0 go build -o /doc-service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /doc-service /doc-service
ENTRYPOINT ["/doc-service"]
