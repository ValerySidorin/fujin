package main

import (
	"fmt"
	"strings"
)

func generateLibraryMain(p pluginsByType) string {
	var imports []string
	imports = append(imports,
		`"errors"`,
		`"fmt"`,
		`"math"`,
		`"time"`,
		`"unsafe"`,
		fmt.Sprintf(`cabi "%s"`, fujinCABI),
	)
	for _, group := range [][]string{p.configurators, p.connectors, p.transports, p.bindMiddlewares, p.connMiddlewares} {
		for _, imp := range group {
			imports = append(imports, fmt.Sprintf(`_ "%s"`, imp))
		}
	}

	var sb strings.Builder
	sb.WriteString(`package main

/*
#include <stdint.h>
#include <stddef.h>
*/
import "C"

import (
`)
	for _, imp := range imports {
		sb.WriteString("\t" + imp + "\n")
	}
	sb.WriteString(`)

var Version string

func init() { cabi.BuildVersion = Version }
func main() {}

//export fujin_abi_version
func fujin_abi_version() C.uint32_t { return C.uint32_t(cabi.ABIVersion) }

//export fujin_v1_start
func fujin_v1_start(
	config unsafe.Pointer,
	configLen C.size_t,
	readyTimeoutMS C.uint64_t,
	outHandle *C.uint64_t,
	errorBuffer *C.char,
	errorCapacity C.size_t,
	errorRequired *C.size_t,
) (result C.uint32_t) {
	clearError(errorBuffer, errorCapacity, errorRequired)
	defer recoverResult(&result, errorBuffer, errorCapacity, errorRequired)
	if outHandle == nil {
		return fail(cabi.ResultInvalidArgument, errors.New("out_handle is NULL"), errorBuffer, errorCapacity, errorRequired)
	}
	*outHandle = 0
	raw, err := copyInput(config, configLen)
	if err != nil {
		return fail(cabi.ResultInvalidArgument, err, errorBuffer, errorCapacity, errorRequired)
	}
	timeout, err := milliseconds(readyTimeoutMS)
	if err != nil {
		return fail(cabi.ResultInvalidArgument, err, errorBuffer, errorCapacity, errorRequired)
	}
	handle, code, err := cabi.Default.Start(raw, timeout)
	if err != nil {
		return fail(code, err, errorBuffer, errorCapacity, errorRequired)
	}
	*outHandle = C.uint64_t(handle)
	return C.uint32_t(cabi.ResultOK)
}

//export fujin_v1_status
func fujin_v1_status(
	handle C.uint64_t,
	outBuffer unsafe.Pointer,
	outCapacity C.size_t,
	outRequired *C.size_t,
	errorBuffer *C.char,
	errorCapacity C.size_t,
	errorRequired *C.size_t,
) (result C.uint32_t) {
	clearError(errorBuffer, errorCapacity, errorRequired)
	defer recoverResult(&result, errorBuffer, errorCapacity, errorRequired)
	if outRequired == nil {
		return fail(cabi.ResultInvalidArgument, errors.New("out_required is NULL"), errorBuffer, errorCapacity, errorRequired)
	}
	payload, code, err := cabi.Default.StatusJSON(uint64(handle))
	if err != nil {
		return fail(code, err, errorBuffer, errorCapacity, errorRequired)
	}
	*outRequired = C.size_t(len(payload))
	if outBuffer == nil || uint64(outCapacity) < uint64(len(payload)) {
		return C.uint32_t(cabi.ResultBufferTooSmall)
	}
	copy(unsafe.Slice((*byte)(outBuffer), len(payload)), payload)
	return C.uint32_t(cabi.ResultOK)
}

//export fujin_v1_apply_connector_snapshot
func fujin_v1_apply_connector_snapshot(
	handle C.uint64_t,
	revision C.uint64_t,
	snapshot unsafe.Pointer,
	snapshotLen C.size_t,
	outState *C.uint32_t,
	outChanged *C.uint8_t,
	errorBuffer *C.char,
	errorCapacity C.size_t,
	errorRequired *C.size_t,
) (result C.uint32_t) {
	clearError(errorBuffer, errorCapacity, errorRequired)
	defer recoverResult(&result, errorBuffer, errorCapacity, errorRequired)
	if outState == nil || outChanged == nil {
		return fail(cabi.ResultInvalidArgument, errors.New("snapshot outputs are NULL"), errorBuffer, errorCapacity, errorRequired)
	}
	*outState = C.uint32_t(cabi.SnapshotRejected)
	*outChanged = 0
	raw, err := copyInput(snapshot, snapshotLen)
	if err != nil {
		return fail(cabi.ResultInvalidArgument, err, errorBuffer, errorCapacity, errorRequired)
	}
	state, changed, code, err := cabi.Default.ApplyConnectorSnapshot(uint64(handle), uint64(revision), raw)
	*outState = C.uint32_t(state)
	if changed {
		*outChanged = 1
	}
	if err != nil {
		return fail(code, err, errorBuffer, errorCapacity, errorRequired)
	}
	return C.uint32_t(cabi.ResultOK)
}

//export fujin_v1_stop
func fujin_v1_stop(
	handle C.uint64_t,
	timeoutMS C.uint64_t,
	errorBuffer *C.char,
	errorCapacity C.size_t,
	errorRequired *C.size_t,
) (result C.uint32_t) {
	clearError(errorBuffer, errorCapacity, errorRequired)
	defer recoverResult(&result, errorBuffer, errorCapacity, errorRequired)
	timeout, err := milliseconds(timeoutMS)
	if err != nil {
		return fail(cabi.ResultInvalidArgument, err, errorBuffer, errorCapacity, errorRequired)
	}
	code, err := cabi.Default.Stop(uint64(handle), timeout)
	if err != nil {
		return fail(code, err, errorBuffer, errorCapacity, errorRequired)
	}
	return C.uint32_t(cabi.ResultOK)
}

func copyInput(pointer unsafe.Pointer, length C.size_t) ([]byte, error) {
	if length == 0 {
		return nil, errors.New("input is empty")
	}
	if pointer == nil {
		return nil, errors.New("input pointer is NULL")
	}
	if uint64(length) > uint64(math.MaxInt) {
		return nil, errors.New("input exceeds addressable memory")
	}
	return append([]byte(nil), unsafe.Slice((*byte)(pointer), int(length))...), nil
}

func milliseconds(value C.uint64_t) (time.Duration, error) {
	if uint64(value) > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return 0, errors.New("timeout overflows time.Duration")
	}
	return time.Duration(value) * time.Millisecond, nil
}

func clearError(buffer *C.char, capacity C.size_t, required *C.size_t) {
	if required != nil {
		*required = 0
	}
	if buffer != nil && capacity > 0 {
		*buffer = 0
	}
}

func fail(code cabi.Result, err error, buffer *C.char, capacity C.size_t, required *C.size_t) C.uint32_t {
	writeError(err, buffer, capacity, required)
	return C.uint32_t(code)
}

func writeError(err error, buffer *C.char, capacity C.size_t, required *C.size_t) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	need := len(message) + 1
	if required != nil {
		*required = C.size_t(need)
	}
	if buffer == nil || capacity == 0 {
		return
	}
	available := int(capacity)
	if available <= 0 {
		return
	}
	output := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), available)
	written := copy(output[:available-1], message)
	output[written] = 0
}

func recoverResult(result *C.uint32_t, buffer *C.char, capacity C.size_t, required *C.size_t) {
	if recovered := recover(); recovered != nil {
		writeError(fmt.Errorf("panic in Fujin C ABI: %v", recovered), buffer, capacity, required)
		*result = C.uint32_t(cabi.ResultPanic)
	}
}
`)
	return sb.String()
}
