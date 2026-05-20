FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.work go.work
COPY pkg/ pkg/
COPY proto/ proto/
COPY user-service/ user-service/
RUN cd user-service && go mod download
RUN cd user-service && CGO_ENABLED=0 go build -o /user-service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /user-service /user-service
ENTRYPOINT ["/user-service"]
