FROM golang:1.18 AS BUILDER

WORKDIR /usr/src/app

COPY go.* cmd/exporter/go.* ./
RUN go mod download

COPY . .
RUN cd ./cmd/exporter && go build .

FROM debian:bullseye

COPY --from=BUILDER /usr/src/app/cmd/exporter/exporter .

CMD ["./exporter"]
