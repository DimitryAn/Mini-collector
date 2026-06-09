FROM golang:1.25 AS builder

WORKDIR /service

COPY go.mod go.sum ./
RUN go mod download 
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /service/main ./cmd/main.go

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /service/main /main
COPY --from=builder /service/.env /.env
COPY --from=builder /service/internal/migrations /internal/migrations
EXPOSE 8080
ENTRYPOINT ["/main"]