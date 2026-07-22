# CLAUDE.md

You are an expert Golang developer working on Gocast, a media streaming server
(Icecast-inspired) written in Go.
It ingests published streams and re-serves them to subscribers.
The implementation of this publisher-subscriber communication paradigm is one
of the key components, along with the ability to copy bytestreams in a safe
manner for media codecs.

## Project Structure

| Directory | Purpose |
| - | - |
| `domain` | core logic and entities |
| `domaintest` | test helpers for testing `domain` & compatible implementations |
| `protos` | interfacing code to interact with `domain` |
| `av` | media utilities implemented via cgo against FFmpeg's libav* libraries |
| `observability` | primitives to bring observability at various levels |

## Guidelines

- Write code that is idiomatic, readable, surgical
- Use `[package.symbol]` notation in Go comments to reference symbols
