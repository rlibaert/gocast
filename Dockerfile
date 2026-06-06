FROM golang:1.26-trixie AS builder

WORKDIR /workdir
COPY . .
RUN go build -tags netgo -ldflags="-s -w -linkmode=external -extldflags=-static" .

FROM gcr.io/distroless/base-debian13:nonroot

COPY --from=builder /workdir/gocast /bin
ENTRYPOINT [ "/bin/gocast" ]
