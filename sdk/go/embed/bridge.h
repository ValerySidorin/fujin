#ifndef FUJIN_GO_BRIDGE_H
#define FUJIN_GO_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

typedef struct FujinHandle FujinHandle;
typedef struct FujinError FujinError;
typedef struct FujinBuffer {
    uint8_t *data;
    size_t length;
} FujinBuffer;

typedef struct FujinLibrary {
    void *handle;
    uint32_t (*abi_version)(void);
    const char *(*build_version)(void);
    int32_t (*start)(const uint8_t *, size_t, FujinHandle **, FujinError **);
    int32_t (*shutdown)(FujinHandle *, FujinError **);
    int32_t (*wait)(FujinHandle *, FujinError **);
    int32_t (*free_handle)(FujinHandle *, FujinError **);
    int32_t (*endpoints_json)(FujinHandle *, FujinBuffer *, FujinError **);
    int32_t (*runtime_status_json)(FujinHandle *, FujinBuffer *, FujinError **);
    int32_t (*watches_connectors)(FujinHandle *, uint8_t *, FujinError **);
    int32_t (*reload_connectors_json)(FujinHandle *, const uint8_t *, size_t, FujinBuffer *, FujinError **);
    int32_t (*reload_from_configurator_json)(FujinHandle *, FujinBuffer *, FujinError **);
    void (*buffer_free)(FujinBuffer);
    const char *(*error_message)(const FujinError *);
    void (*error_free)(FujinError *);
} FujinLibrary;

FujinLibrary *fujin_library_open(const char *path, char *error, size_t error_capacity);
void fujin_library_close(FujinLibrary *library);
uint32_t fujin_call_abi_version(FujinLibrary *library);
const char *fujin_call_build_version(FujinLibrary *library);
int32_t fujin_call_start(FujinLibrary *, const uint8_t *, size_t, FujinHandle **, FujinError **);
int32_t fujin_call_shutdown(FujinLibrary *, FujinHandle *, FujinError **);
int32_t fujin_call_wait(FujinLibrary *, FujinHandle *, FujinError **);
int32_t fujin_call_free(FujinLibrary *, FujinHandle *, FujinError **);
int32_t fujin_call_endpoints_json(FujinLibrary *, FujinHandle *, FujinBuffer *, FujinError **);
int32_t fujin_call_runtime_status_json(FujinLibrary *, FujinHandle *, FujinBuffer *, FujinError **);
int32_t fujin_call_watches_connectors(FujinLibrary *, FujinHandle *, uint8_t *, FujinError **);
int32_t fujin_call_reload_connectors_json(FujinLibrary *, FujinHandle *, const uint8_t *, size_t, FujinBuffer *, FujinError **);
int32_t fujin_call_reload_from_configurator_json(FujinLibrary *, FujinHandle *, FujinBuffer *, FujinError **);
void fujin_call_buffer_free(FujinLibrary *, FujinBuffer);
const char *fujin_call_error_message(FujinLibrary *, const FujinError *);
void fujin_call_error_free(FujinLibrary *, FujinError *);

#endif
