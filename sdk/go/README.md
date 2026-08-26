# Fujin Go SDKs

The Fujin repository contains two independently versioned Go modules:

| Module | Purpose | Module path | Release tag |
| --- | --- | --- | --- |
| [Client](client) | Native QUIC and protobuf gRPC client | `github.com/fujin-io/fujin/sdk/go/client` | `sdk/go/client/vX.Y.Z` |
| [Embedding](embed) | cgo controls for a generated Fujin library | `github.com/fujin-io/fujin/sdk/go/embed` | `sdk/go/embed/vX.Y.Z` |

They remain separate modules so users of the embedding SDK do not inherit QUIC, gRPC, and protobuf
dependencies from the network client. `go.work` joins them for repository development only; neither
published module depends on the workspace file.

The server protobuf at [`proto/grpc/v1/fujin.proto`](../../proto/grpc/v1/fujin.proto) is the single
source for the client bindings. Run `make generate` from the repository root after changing it.
`make sdk-test` tests both modules, and `make sdk-compat` builds a current Rust server fixture and
exercises both client adapters end to end.

Go module releases use namespaced tags and are independent of Fujin server and container releases.
A client release is required only when its public API or supported wire contract changes. An
embedding release is required only when its Go API or supported C ABI contract changes.
