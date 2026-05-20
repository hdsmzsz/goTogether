FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.work go.work
COPY pkg/ pkg/
COPY proto/ proto/
COPY collab-service/ collab-service/
RUN cd collab-service && go mod download
RUN cd collab-service && CGO_ENABLED=0 go build -o /collab-service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /collab-service /collab-service
ENTRYPOINT ["/collab-service"]
