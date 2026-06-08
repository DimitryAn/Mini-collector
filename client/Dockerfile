FROM golang:1.25

WORKDIR /client

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /client/cmd/main ./cmd/main.go

CMD ["sh", "-c", "sleep 10 && exec /client/cmd/main"]