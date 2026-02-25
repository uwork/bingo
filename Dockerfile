# ---- Build Stage ----
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bingo .

# ---- Runtime Stage ----
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /bingo /usr/local/bin/bingo

ENTRYPOINT ["bingo"]
