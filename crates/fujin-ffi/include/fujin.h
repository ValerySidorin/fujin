#ifndef FUJIN_H
#define FUJIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FujinHandle FujinHandle;
typedef struct FujinError FujinError;

typedef struct FujinBuffer {
    uint8_t *data;
    size_t length;
} FujinBuffer;

enum FujinStatus {
    FUJIN_STATUS_OK = 0,
    FUJIN_STATUS_INVALID_ARGUMENT = 1,
    FUJIN_STATUS_START_FAILED = 2,
    FUJIN_STATUS_RUNTIME_FAILED = 3,
    FUJIN_STATUS_PANIC = 255
};

uint32_t fujin_abi_version_v1(void);
const char *fujin_build_version_v1(void);

int32_t fujin_start_v1(
    const uint8_t *request,
    size_t request_length,
    FujinHandle **output,
    FujinError **error);
int32_t fujin_shutdown_v1(FujinHandle *handle, FujinError **error);
int32_t fujin_wait_v1(FujinHandle *handle, FujinError **error);
int32_t fujin_free_v1(FujinHandle *handle, FujinError **error);

int32_t fujin_endpoints_json_v1(
    FujinHandle *handle,
    FujinBuffer *output,
    FujinError **error);
int32_t fujin_runtime_status_json_v1(
    FujinHandle *handle,
    FujinBuffer *output,
    FujinError **error);
int32_t fujin_watches_connectors_v1(
    FujinHandle *handle,
    uint8_t *output,
    FujinError **error);
int32_t fujin_reload_connectors_json_v1(
    FujinHandle *handle,
    const uint8_t *request,
    size_t request_length,
    FujinBuffer *output,
    FujinError **error);
int32_t fujin_reload_from_configurator_json_v1(
    FujinHandle *handle,
    FujinBuffer *output,
    FujinError **error);

void fujin_buffer_free_v1(FujinBuffer buffer);
const char *fujin_error_message_v1(const FujinError *error);
void fujin_error_free_v1(FujinError *error);

#ifdef __cplusplus
}
#endif

#endif
