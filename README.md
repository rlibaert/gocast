# Gocast

[![CI](https://github.com/rlibaert/gocast/actions/workflows/golangci-lint.yaml/badge.svg)](https://github.com/rlibaert/gocast/actions/workflows/golangci-lint.yaml)
[![CI](https://github.com/rlibaert/gocast/actions/workflows/go-test.yaml/badge.svg)](https://github.com/rlibaert/gocast/actions/workflows/go-test.yaml)

**Gocast** is a simple media streaming server, inspired by [Icecast].

This project is an attempt to use up-to-date technology to offer a modern take
on Internet radio streaming:

- High scalability out of the box thanks to Go's concurrency model
- Multi-protocol support (HTTP, ICY, SRT)
- Prometheus metrics
- Cloud-native development approach (somewhat)

> [!NOTE]
> While there is no intention to imitate [Icecast], some of its features are
> reimplemented. The primary motivation is to not break compatibility with
> existing software.

## Getting started

By default, the server listens on ports:

- `8080` for HTTP
- `8000` for [Icecast]-style HTTP interface (ICY)
- `6000` for [Secure Reliable Transport] (SRT)

The latest binary is available on the [releases page](https://github.com/rlibaert/gocast/releases)
or you can use the [container image](https://github.com/rlibaert/gocast/pkgs/container/gocast):

```bash
docker run --rm -p 8080:8080 -p 8000:8000 -p 6000/udp:6000/udp ghcr.io/rlibaert/gocast:latest
```

You can use an Icecast-compatible client to broadcast to the server on port
`8000` or rely on more exotic setups, for example using FFmpeg:

```bash
ffmpeg -i ... http://localhost:8080/streams/foo.mp3
ffmpeg -i ... srt://localhost:6000?streamid=publish:foo.mp3
```

The streams may then be consumed:

```bash
ffplay -i http://localhost:8080/streams/foo.mp3
ffplay -i srt://localhost:6000?streamid=foo.mp3
```

If your client is compatible with Icecast's in-band metadata, they will be
served:

```bash
$ mpv http://localhost:8000/foo.mp3
File tags:
 icy-title: foo
```

> [!TIP]
> You can check out the [deployment](deployment/) directory for a complete yet
> simple docker-compose based setup.

[Icecast]: https://icecast.org/
[Secure Reliable Transport]: https://en.wikipedia.org/wiki/Secure_Reliable_Transport
