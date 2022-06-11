FROM golang:1.18

WORKDIR /usr/src/app

COPY go.* cmd/exporter/go.* ./
RUN go mod download

COPY . .
RUN cd ./cmd/exporter && go build .

CMD ["./cmd/exporter/exporter"]
