# WebAssembly Transform Middleware

Connector middleware that transforms message payloads with a SHA-256-pinned WebAssembly module executed by [wazero](https://wazero.io/).

- **Produce/HProduce:** transforms the payload before passing it to the connector. A failed transform rejects the operation with `TransformError`.
- **Subscribe/Fetch:** transforms payloads received from the connector. A failed transform skips the message and writes a warning log.
- Headers, source identifiers, message IDs, acknowledgements, and transaction operations are not transformed.

**Registered name:** `transform_wasm`

## Configuration

```yaml
connectors:
  my_connector:
    type: kafka_franz
    connector_middlewares:
      - name: transform_wasm
        path: ./plugins/transform.wasm
        sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        produce_function: transform_produce
        consume_function: transform_consume
        timeout: 5ms
        max_memory_mb: 16
        max_output_bytes: 4194304
    settings:
      # Connector-specific settings.
```

| Field | Default | Description |
|---|---:|---|
| `path` | required | Path to the `.wasm` module |
| `sha256` | required | Expected lowercase or uppercase 64-character hexadecimal SHA-256 digest |
| `produce_function` | — | Export called for Produce and HProduce payloads |
| `consume_function` | — | Export called for Subscribe and Fetch payloads |
| `timeout` | `5ms` | Maximum duration of each guest function call |
| `max_memory_mb` | `16` | Maximum WebAssembly linear memory, rounded to 64 KiB pages; maximum configurable value is `4096` |
| `max_output_bytes` | `4194304` | Maximum transformed payload size |

At least one of `produce_function` or `consume_function` is required. An omitted direction passes payloads through unchanged.

## Required Guest ABI

The module must export linear memory named `memory` and the following functions:

```text
alloc(length: i32) -> i32
dealloc(pointer: i32, length: i32)
<configured transform>(pointer: i32, length: i32) -> i64
```

The transform result packs the output pointer and length into one `i64`:

```text
bits 63..32: output pointer
bits 31..0:  output length
```

Equivalent guest code:

```rust
((output_pointer as u64) << 32) | output_length as u64
```

The transform may modify the input in place and return the input pointer, or allocate and return a different output buffer.

### Memory Ownership

For each message Fujin:

1. calls `alloc(input_length)`;
2. copies the input payload into guest memory;
3. calls the configured transform with the input pointer and length;
4. validates and copies the output from guest memory into Go-owned memory;
5. calls `dealloc` for the input allocation;
6. also calls `dealloc` for a distinct output allocation.

When the transform returns the input pointer, Fujin deallocates it once using the original input length. Guest allocators must keep their `alloc` and `dealloc` conventions compatible with these lengths.

## Rust Example

The complete example is in [`examples/plugins/middleware/connector/wasm-uppercase`](../../../../../../examples/plugins/middleware/connector/wasm-uppercase).

```rust
use std::{mem, slice};

#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    if len == 0 {
        return 0;
    }
    let mut buffer = Vec::<u8>::with_capacity(len as usize);
    let pointer = buffer.as_mut_ptr() as u32;
    mem::forget(buffer);
    pointer
}

#[no_mangle]
pub unsafe extern "C" fn dealloc(pointer: u32, len: u32) {
    if len != 0 {
        drop(Vec::from_raw_parts(pointer as *mut u8, 0, len as usize));
    }
}

#[no_mangle]
pub unsafe extern "C" fn transform(pointer: u32, len: u32) -> u64 {
    if len == 0 {
        return 0;
    }
    let message = slice::from_raw_parts_mut(pointer as *mut u8, len as usize);
    message.make_ascii_uppercase();
    ((pointer as u64) << 32) | len as u64
}
```

Build and hash it:

```bash
rustup target add wasm32-unknown-unknown
cargo build \
  --manifest-path examples/plugins/middleware/connector/wasm-uppercase/Cargo.toml \
  --release \
  --target wasm32-unknown-unknown
shasum -a 256 \
  examples/plugins/middleware/connector/wasm-uppercase/target/wasm32-unknown-unknown/release/fujin_wasm_uppercase.wasm
```

The crate must use `crate-type = ["cdylib"]`. `panic = "abort"` is recommended because there is no host panic or exception integration.

## Validation and Publication

`transform_wasm` is generation-scoped compiled middleware. Before a configuration generation is published, Fujin:

1. reads the module from `path`;
2. verifies its SHA-256 digest using a constant-time comparison;
3. compiles it with wazero's compiler runtime;
4. rejects every imported function or memory;
5. validates the exported memory and function signatures;
6. instantiates one module for the configured middleware instance.

A missing file, digest mismatch, invalid module, missing export, or incompatible signature rejects the new configuration generation before publication. Replacing the module file requires updating `sha256` in the configuration.

The digest provides integrity pinning, not author trust. Only deploy modules produced or reviewed by a trusted party.

## Sandbox

Guest modules may not import host functions or host memories. Fujin does not instantiate WASI, so the guest has no Fujin-provided access to:

- files or directories;
- environment variables or arguments;
- sockets or network services;
- clocks, randomness, or process APIs;
- host callbacks.

The guest can still keep mutable WebAssembly globals and linear-memory state between calls. Treat the transform as stateful unless its implementation is known to be pure.

## Concurrency and Performance

One module instance is shared by all reader and writer wrappers for this configured middleware in one connector generation. Produce and consume transforms use one mutex because they share guest memory and globals. Guest calls are therefore serialized.

Consequences:

- a slow transform delays other transforms using the same middleware instance;
- a CPU-heavy transform can become the connector's throughput bottleneck;
- each operation copies the input into guest memory and copies the output back into Go memory;
- separate module instances are created only by separate configured middleware instances or configuration generations.

Keep transforms bounded and use the smallest practical `max_memory_mb`, `max_output_bytes`, and `timeout` values.

## Failure Semantics

### Produce and HProduce

If allocation, memory access, transform execution, output validation, or deallocation fails:

- the connector writer is not called;
- the operation callback receives `TransformError`;
- Fujin logs a warning;
- there is no automatic retry or fallback to the original payload.

### Subscribe and Fetch

If transformation fails:

- the message handler is not called for that message;
- Fujin logs a warning containing the source;
- processing continues for later messages when the module remains usable.

Acknowledgement, negative acknowledgement, flush, and transaction methods pass through unchanged.

### Timeout Behavior

The wazero runtime uses `WithCloseOnContextDone(true)`. If a guest call exceeds `timeout` or its parent context is canceled during execution, wazero terminates the call and closes that module instance. Later transforms using the same instance fail until a new configuration generation creates a replacement module.

Monitor transform warnings: a timeout is not merely one dropped or rejected message; it can make the current middleware instance unusable.

## Reload and Shutdown

Every successfully compiled connector generation owns its WASM runtime and module. Existing sessions continue using their pinned generation. When that generation is retired and closed, Fujin closes the wazero runtime and releases its compiled code and memory.
