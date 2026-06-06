FROM golang:1.26-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential nasm

ADD http://ffmpeg.org/releases/ffmpeg-8.1.tar.xz .
RUN tar -xJf ffmpeg-8.1.tar.xz && \
    cd ffmpeg-8.1 && \
    ./configure \
        --disable-all \
        --enable-avutil --enable-avformat --enable-avcodec \
        --enable-demuxer=mp3,aac \
        --enable-decoder=mp3*,aac* \
    && \
    make -j$(nproc) && \
    make install

WORKDIR /workdir
COPY . .
RUN CGO_ENABLED=1 && \
    go build -ldflags="-s -w -linkmode=external -extldflags=-static" .

FROM gcr.io/distroless/base-debian13:nonroot
COPY --from=builder /workdir/gocast /bin
ENTRYPOINT [ "/bin/gocast" ]
