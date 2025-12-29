FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /golt ./cmd/golt

FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /golt /app/golt

RUN mkdir -p /data

EXPOSE 7000 8000

ENTRYPOINT ["/app/golt"]
