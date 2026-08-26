#include "bridge.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>

static void *open_library(const char *path, char *error, size_t error_capacity) {
    HMODULE handle = LoadLibraryA(path);
    if (handle == NULL) {
        snprintf(error, error_capacity, "load Fujin library %s: Windows error %lu", path,
                 (unsigned long)GetLastError());
    }
    return (void *)handle;
}

static void close_library(void *handle) { FreeLibrary((HMODULE)handle); }

static void *load_symbol(void *handle, const char *name, char *error, size_t error_capacity) {
    FARPROC symbol = GetProcAddress((HMODULE)handle, name);
    if (symbol == NULL) {
        snprintf(error, error_capacity, "resolve %s: Windows error %lu", name,
                 (unsigned long)GetLastError());
    }
    return (void *)(uintptr_t)symbol;
}
#else
#include <dlfcn.h>

static void *open_library(const char *path, char *error, size_t error_capacity) {
    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        snprintf(error, error_capacity, "load Fujin library %s: %s", path, dlerror());
    }
    return handle;
}

static void close_library(void *handle) { dlclose(handle); }

static void *load_symbol(void *handle, const char *name, char *error, size_t error_capacity) {
    dlerror();
    void *symbol = dlsym(handle, name);
    const char *message = dlerror();
    if (message != NULL) {
        snprintf(error, error_capacity, "resolve %s: %s", name, message);
        return NULL;
    }
    return symbol;
}
#endif

#define LOAD_SYMBOL(target, name)                                                    \
    do {                                                                             \
        *(void **)(&(target)) = load_symbol(library->handle, name, error, error_capacity); \
        if ((target) == NULL) {                                                       \
            fujin_library_close(library);                                              \
            return NULL;                                                              \
        }                                                                             \
    } while (0)

FujinLibrary *fujin_library_open(const char *path, char *error, size_t error_capacity) {
    if (path == NULL || path[0] == '\0') {
        snprintf(error, error_capacity, "Fujin library path is empty");
        return NULL;
    }
    FujinLibrary *library = calloc(1, sizeof(*library));
    if (library == NULL) {
        snprintf(error, error_capacity, "allocate Fujin library bridge");
        return NULL;
    }
    library->handle = open_library(path, error, error_capacity);
    if (library->handle == NULL) {
        free(library);
        return NULL;
    }
    LOAD_SYMBOL(library->abi_version, "fujin_abi_version_v1");
    LOAD_SYMBOL(library->build_version, "fujin_build_version_v1");
    LOAD_SYMBOL(library->start, "fujin_start_v1");
    LOAD_SYMBOL(library->shutdown, "fujin_shutdown_v1");
    LOAD_SYMBOL(library->wait, "fujin_wait_v1");
    LOAD_SYMBOL(library->free_handle, "fujin_free_v1");
    LOAD_SYMBOL(library->endpoints_json, "fujin_endpoints_json_v1");
    LOAD_SYMBOL(library->runtime_status_json, "fujin_runtime_status_json_v1");
    LOAD_SYMBOL(library->watches_connectors, "fujin_watches_connectors_v1");
    LOAD_SYMBOL(library->reload_connectors_json, "fujin_reload_connectors_json_v1");
    LOAD_SYMBOL(library->reload_from_configurator_json, "fujin_reload_from_configurator_json_v1");
    LOAD_SYMBOL(library->buffer_free, "fujin_buffer_free_v1");
    LOAD_SYMBOL(library->error_message, "fujin_error_message_v1");
    LOAD_SYMBOL(library->error_free, "fujin_error_free_v1");
    return library;
}

void fujin_library_close(FujinLibrary *library) {
    if (library == NULL) {
        return;
    }
    if (library->handle != NULL) {
        close_library(library->handle);
    }
    free(library);
}

uint32_t fujin_call_abi_version(FujinLibrary *library) { return library->abi_version(); }
const char *fujin_call_build_version(FujinLibrary *library) { return library->build_version(); }

int32_t fujin_call_start(FujinLibrary *library, const uint8_t *request, size_t request_length,
                         FujinHandle **output, FujinError **error) {
    return library->start(request, request_length, output, error);
}
int32_t fujin_call_shutdown(FujinLibrary *library, FujinHandle *handle, FujinError **error) {
    return library->shutdown(handle, error);
}
int32_t fujin_call_wait(FujinLibrary *library, FujinHandle *handle, FujinError **error) {
    return library->wait(handle, error);
}
int32_t fujin_call_free(FujinLibrary *library, FujinHandle *handle, FujinError **error) {
    return library->free_handle(handle, error);
}
int32_t fujin_call_endpoints_json(FujinLibrary *library, FujinHandle *handle,
                                  FujinBuffer *output, FujinError **error) {
    return library->endpoints_json(handle, output, error);
}
int32_t fujin_call_runtime_status_json(FujinLibrary *library, FujinHandle *handle,
                                       FujinBuffer *output, FujinError **error) {
    return library->runtime_status_json(handle, output, error);
}
int32_t fujin_call_watches_connectors(FujinLibrary *library, FujinHandle *handle,
                                      uint8_t *output, FujinError **error) {
    return library->watches_connectors(handle, output, error);
}
int32_t fujin_call_reload_connectors_json(FujinLibrary *library, FujinHandle *handle,
                                          const uint8_t *request, size_t request_length,
                                          FujinBuffer *output, FujinError **error) {
    return library->reload_connectors_json(handle, request, request_length, output, error);
}
int32_t fujin_call_reload_from_configurator_json(FujinLibrary *library, FujinHandle *handle,
                                                  FujinBuffer *output, FujinError **error) {
    return library->reload_from_configurator_json(handle, output, error);
}
void fujin_call_buffer_free(FujinLibrary *library, FujinBuffer buffer) {
    library->buffer_free(buffer);
}
const char *fujin_call_error_message(FujinLibrary *library, const FujinError *error) {
    return library->error_message(error);
}
void fujin_call_error_free(FujinLibrary *library, FujinError *error) {
    library->error_free(error);
}
