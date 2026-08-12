# Certify post-cutover performance against the original baseline

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Run the complete approved performance matrix after the Session Core cutover and compare it statistically with the authoritative pre-refactor baseline on the same machine and environment. Investigate and remove every confirmed regression before declaring the extraction complete.

## User stories covered

- Prove that the shared core did not reduce performance.
- Detect regressions hidden at small message counts or a single payload size.
- Retain auditable evidence for future optimizations.

## Acceptance criteria

- [x] Re-run the same commands, sample counts, payload sizes, operation scales, batch sizes, transports, and concurrency profiles as the baseline.
- [x] Use the same OS, CPU, Go toolchain, `GOMAXPROCS`, build tags, and machine power/thermal conditions, or document and rerun any invalid comparison.
- [x] Benchstat reports no statistically significant negative delta in `ns/op` or throughput for any required cell.
- [x] p99 latency does not regress for any required load profile.
- [x] `B/op` and `allocs/op` do not increase for any migrated path.
- [x] Any inconclusive cell is rerun; any confirmed regression blocks completion and is fixed at its source.
- [x] Raw before/after results, environment fingerprints, and comparison reports are retained together.
- [x] Native protocol payloads of at least `1 MiB` remain functional under sustained load.

## Certification evidence

- Verdict and authoritative artifact index: `test/performance/certification-2026-08-12-final/manifest.txt`.
- Environment fingerprint: `test/performance/certification-2026-08-12-final/environment.txt`.
- Native 128 B matrix: `test/performance/certification-2026-08-12-final/comparison-native-128B-allops-alltransports-allconcurrency-1000x-10-interleaved-final-head.txt`.
- gRPC 128 B matrix: `test/performance/certification-2026-08-12-final/comparison-grpc-128B-allops-allconcurrency-1000x-10-interleaved.txt`.
- Payload and batch spectra: `test/performance/certification-2026-08-12-final/comparison-native-produce-payload-spectrum-c1-100x-10-interleaved-post-local-response.txt`, `test/performance/certification-2026-08-11/comparison-final-post-backpressure-native-batches-c16-10x.txt`, and `test/performance/certification-2026-08-11/comparison-batch-spectrum-1B-c16-10x.txt`.
- Fixed operation scales: the four `comparison-operation-scale-*` reports indexed by the manifest.
- Final aggregate-suspect rerun: `test/performance/certification-2026-08-12-final/comparison-native-final-suspects-20-balanced-final-source.txt`.
- Final QUIC ACK/NACK rerun: `test/performance/certification-2026-08-12-final/comparison-native-ack-nack-quic-128B-c128-100000x-20-balanced-lock-free-final.txt`.
- Sustained 1 MiB native result: `test/performance/certification-2026-08-12-final/current-sustained-native-produce-hsubscribe-1MiB-alltransports-c16-10000x.bench`.
- Final correctness verification: `test/performance/certification-2026-08-12-final/verification.txt`; default, `fujin`, `grpc`, and `fujin,grpc` repository suites pass, all 151 benchmark entry points and 876 Session cells execute, and all nine opt-in broker-backed E2E suites pass.
- Post-fix machine-readable audit: `test/performance/certification-2026-08-12-final/current-audit/goal-postfix-final-artifact-audit.json`; it includes the final benchmark coverage, build-tag matrix, broker E2E matrix, 12 regenerated reports, sample counts, sustained 1 MiB cells, evidence references, and raw failure-marker scan.

The final focused reports supersede statistically significant outliers in earlier aggregate and diagnostic reports. On final source, no authoritative cell has a negative `ns/op`, throughput, p99, `B/op`, or `allocs/op` verdict. Raw final-source files contain no benchmark failure, panic, timeout, or fatal-error output. The legacy Produce benchmark validator was corrected for Go 1.26 `B.Loop` semantics, and RabbitMQ/Artemis Compose services now expose protocol-aware healthchecks; both harness changes are covered by the post-fix benchmark and E2E audits.

## Blocked by

- `issues/009-remove-duplicate-session-implementations.md`
