FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.work go.work
COPY pkg/ pkg/
COPY proto/ proto/
COPY gateway/ gateway/
RUN cd gateway && go mod download
RUN cd gateway && CGO_ENABLED=0 go build -o /gateway .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /gateway /gateway
COPY web/ /app/web/
WORKDIR /app
ENTRYPOINT ["/gateway"]
