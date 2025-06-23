FROM golang:latest AS builder

WORKDIR /app

COPY go.mod .

RUN go mod download

COPY . .

RUN go build -o search-service .

FROM debian:unstable-slim

WORKDIR /app

COPY --from=builder /app/search-service .

EXPOSE 50050

CMD ["./search-service"]