# Implement library-specific ZeroMQ connectors

**Type:** Feature
**Label:** `needs-discussion`

## Architectural decision

ZeroMQ integrations follow the existing Fujin plugin naming convention: a connector is identified by the concrete client library it uses, not only by the broker or protocol family.

Consequences:

- Do not register a generic `zeromq` plugin type.
- Do not introduce a shared backend abstraction merely to hide differences between ZeroMQ libraries.
- Each implementation has its own package, registration name, configuration capabilities, dependencies, build behavior, tests, and documentation.
- Multiple implementations may coexist and users select one explicitly in `connectors.<name>.type`.
- Library-specific limitations such as CGO, CURVE support, socket options, transports, and cross-compilation remain visible rather than being reduced to a lowest-common-denominator API.

Candidate shape, subject to naming review during implementation:

```text
public/plugins/connector/zeromq/gozeromq  -> zeromq_gozeromq
public/plugins/connector/zeromq/pebbe     -> zeromq_pebbe
```

The first path would use `github.com/go-zeromq/zmq4`; the second would use `github.com/pebbe/zmq4` and native `libzmq`. Adding one does not require implementing the other.

## Planned work

- Define the supported ZeroMQ socket-pattern and message-framing contract before selecting the first library implementation.
- Implement the first connector under its library-specific package and plugin name.
- Add it explicitly to the full-plugin import set and custom builder documentation.
- Add library-specific configuration, validation, lifecycle, interoperability tests, E2E fixtures, and benchmarks.
- Replace the current generic `zeromq` claim in user documentation with the exact implemented plugin name or names.
- Add real Make and Compose targets for the interoperability fixture; ZeroMQ itself is not treated as a standalone broker service.

## Open design questions

- Which socket patterns belong in the first implementation.
- How Fujin routes, headers, and ZeroMQ multipart frames map to each other.
- Whether Fujin sockets connect, bind, or support both ownership modes.
- Required delivery semantics, backpressure behavior, security, and reconnect policy.
- Which library implementation should be delivered first.
