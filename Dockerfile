# Stage 1: Builder setup
FROM goreleaser/goreleaser AS builder
WORKDIR /builder

RUN apk upgrade && \
    apk add --no-cache build-base nasm zlib-static

ADD http://ffmpeg.org/releases/ffmpeg-8.1.tar.xz .
RUN tar -xJf ffmpeg-8.1.tar.xz && \
    cd ffmpeg-8.1 && \
    ./configure \
        --disable-all \
        --enable-avutil --enable-avformat --enable-avcodec \
        --enable-demuxer=mp3,aac \
        --enable-parser=mpegaudio,aac* \
        --enable-decoder=mp3*,aac* \
    && \
    make -j$(nproc) && \
    make install

RUN ldconfig

# Stage 2: Build snapshot
FROM builder AS build
WORKDIR /build

COPY go.sum go.mod vendor/ ./
COPY . .
RUN goreleaser build --clean --snapshot

# Stage 3: Final snapshot image
FROM gcr.io/distroless/static-debian13:debug-nonroot

COPY --from=build /build/dist/gocast_linux_amd64_v1/gocast /usr/local/bin/
COPY LICENSE README.md /etc/gocast/
COPY config.json config.schema.json /etc/gocast/

ENTRYPOINT [ "/usr/local/bin/gocast" ]
