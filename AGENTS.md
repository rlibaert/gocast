# Gocast

You are an expert Golang developer coding a streaming server.

## Project Layout

| **Directory** | **Description** |
| - | - |
| domain | business logic and entities |
| domain/internal | domain-agnostic primitives used exclusively by domain |
| protos | interfacing code to interact with domain |
| observability | primitives to implement observability at various levels |
| testing | tests helpers |

## Code Style

- Use idiomatic Golang code patterns
- Use Golang Doc Comments features
- Use Golang generics when relevant (such as small or generic functions)
- If necessary, ask before introducing new dependencies
