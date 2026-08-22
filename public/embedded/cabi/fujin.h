#ifndef FUJIN_EMBEDDED_V1_H
#define FUJIN_EMBEDDED_V1_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t fujin_handle;
typedef uint32_t fujin_result;
typedef uint32_t fujin_snapshot_state;
enum {
    FUJIN_RESULT_OK = 0,
    FUJIN_RESULT_INVALID_ARGUMENT = 1,
    FUJIN_RESULT_INVALID_HANDLE = 2,
    FUJIN_RESULT_BUFFER_TOO_SMALL = 3,
    FUJIN_RESULT_INVALID_CONFIG = 4,
    FUJIN_RESULT_START_FAILED = 5,
    FUJIN_RESULT_TIMEOUT = 6,
    FUJIN_RESULT_SNAPSHOT_REJECTED = 7,
    FUJIN_RESULT_INTERNAL = 8,
    FUJIN_RESULT_PANIC = 9
};

enum {
    FUJIN_SNAPSHOT_ACCEPTED = 0,
    FUJIN_SNAPSHOT_REJECTED = 1,
    FUJIN_SNAPSHOT_STALE = 2,
    FUJIN_SNAPSHOT_SUPERSEDED = 3
};

/* Returns the highest ABI major implemented by this library. */
uint32_t fujin_abi_version(void);

/*
 * Starts one Fujin runtime from complete YAML or JSON bootstrap configuration.
 * The library copies config before returning. ready_timeout_ms == 0 selects the
 * library default. On success, *out_handle is nonzero and owned by the caller.
 *
 * Error text is optional. When error_required is non-NULL, it receives bytes
 * required including the trailing NUL. error_buffer is always NUL-terminated
 * when error_capacity > 0. Truncation never changes the primary result code.
 */
fujin_result fujin_v1_start(
    const void *config,
    size_t config_len,
    uint64_t ready_timeout_ms,
    fujin_handle *out_handle,
    char *error_buffer,
    size_t error_capacity,
    size_t *error_required);

/*
 * Serializes a versioned UTF-8 JSON status document. The returned byte count
 * excludes a trailing NUL; status is not NUL-terminated. Passing a NULL buffer
 * or insufficient capacity returns FUJIN_RESULT_BUFFER_TOO_SMALL and writes the
 * exact required byte count to out_required.
 */
fujin_result fujin_v1_status(
    fujin_handle handle,
    void *out_buffer,
    size_t out_capacity,
    size_t *out_required,
    char *error_buffer,
    size_t error_capacity,
    size_t *error_required);

/*
 * Applies one complete immutable YAML or JSON connector snapshot. The library
 * copies snapshot before returning. Revision ordering follows Fujin runtime
 * snapshot semantics. out_state and out_changed are required.
 */
fujin_result fujin_v1_apply_connector_snapshot(
    fujin_handle handle,
    uint64_t revision,
    const void *snapshot,
    size_t snapshot_len,
    fujin_snapshot_state *out_state,
    uint8_t *out_changed,
    char *error_buffer,
    size_t error_capacity,
    size_t *error_required);

/*
 * Requests shutdown and releases handle ownership after cleanup completes.
 * timeout_ms == 0 waits without a deadline. A timeout preserves the handle so
 * shutdown can be retried. A successfully stopped handle becomes invalid.
 */
fujin_result fujin_v1_stop(
    fujin_handle handle,
    uint64_t timeout_ms,
    char *error_buffer,
    size_t error_capacity,
    size_t *error_required);

/*
 * All functions are safe to call concurrently with different handles. Calls
 * using one handle are serialized where Fujin lifecycle semantics require it.
 * No Go pointer crosses the ABI. The library must remain loaded until every
 * handle has been stopped; unloading an active Go shared library is unsupported.
 */

#ifdef __cplusplus
}
#endif

#endif
