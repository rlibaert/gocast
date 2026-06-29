# Gocast

[![CI](https://github.com/rlibaert/gocast/actions/workflows/golangci-lint.yaml/badge.svg)](https://github.com/rlibaert/gocast/actions/workflows/golangci-lint.yaml)
[![CI](https://github.com/rlibaert/gocast/actions/workflows/go-test.yaml/badge.svg)](https://github.com/rlibaert/gocast/actions/workflows/go-test.yaml)

**Gocast** is a simple media streaming server, inspired by [Icecast].

This project is an attempt to use up-to-date technology to offer a modern take
on Internet radio streaming.

> [!NOTE]
> While there is no intention to imitate Icecast, some of its features are
> reimplemented. The primary motivation is to not break compatibility with
> existing software.

## Features

- Multi-protocol support (HTTP, ICY, SRT)
- Compatible with Icecast's `PUT` & legacy `SOURCE` protocols as well as
  in-band metadata
- Strong scalability out of the box thanks to Go's concurrency model
- Support for MP3 & AAC
- Sources fallback
- Prometheus metrics

## Getting started

Gocast is available as a [container image](https://github.com/rlibaert/gocast/pkgs/container/gocast)
or a standalone binary on the [releases page](https://github.com/rlibaert/gocast/releases).
You can also easily build a snapshot image from source:

```bash
$ docker build -t gocast .
```

By default, the server listens on ports:

- `8080` for HTTP
- `8000` for Icecast-style HTTP interface (ICY)
- `6000/udp` for [Secure Reliable Transport] (SRT)

```bash
$ docker run --rm -p 8080:8080 -p 8000:8000 -p 6000/udp:6000/udp ghcr.io/rlibaert/gocast
```

You can use an Icecast-compatible client to broadcast to the server on port
`8000` or rely on more exotic setups, for example using FFmpeg:

```bash
$ ffmpeg -i ... "http://localhost:8080/streams/foo.mp3"
$ ffmpeg -i ... "srt://localhost:6000?streamid=publish:foo.mp3"
```

The streams may then be consumed:

```bash
$ ffplay -i "http://localhost:8080/streams/foo.mp3"
$ ffplay -i "srt://localhost:6000?streamid=foo.mp3"
```

Your client will be served with Icecast's in-band metadata if it is compatible:

```bash
$ mpv "http://localhost:8000/foo.mp3"
File tags:
 icy-title: foo
```

> [!TIP]
> You can check out the [deployment](deployment/) directory for a complete yet
> simple docker-compose based setup.

[Icecast]: https://icecast.org/
[Secure Reliable Transport]: https://en.wikipedia.org/wiki/Secure_Reliable_Transport
