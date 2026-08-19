package wasm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero/api"
)

type transformer struct {
	function api.Function
	alloc    api.Function
	dealloc  api.Function
	memory   api.Memory
	config   settings
	mu       *sync.Mutex
}

func newTransformer(middleware *middleware, functionName string) *transformer {
	module := middleware.module
	return &transformer{
		function: module.ExportedFunction(functionName),
		alloc:    module.ExportedFunction("alloc"),
		dealloc:  module.ExportedFunction("dealloc"),
		memory:   module.Memory(),
		config:   middleware.config,
		mu:       &middleware.callMu,
	}
}

func (t *transformer) transform(ctx context.Context, input []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithTimeout(ctx, t.config.Timeout)
	defer cancel()

	allocated, err := t.alloc.Call(callCtx, uint64(len(input)))
	if err != nil {
		return nil, fmt.Errorf("allocate WebAssembly input: %w", err)
	}
	inputPointer := uint32(allocated[0])
	if !t.memory.Write(inputPointer, input) {
		_ = t.deallocate(callCtx, inputPointer, uint32(len(input)))
		return nil, errors.New("write WebAssembly input outside memory")
	}
	result, callErr := t.function.Call(callCtx, uint64(inputPointer), uint64(len(input)))
	if callErr != nil {
		return nil, fmt.Errorf("call WebAssembly transform: %w", callErr)
	}
	outputPointer := uint32(result[0] >> 32)
	outputLength := uint32(result[0])
	if outputLength > t.config.MaxOutputBytes {
		_ = t.deallocate(callCtx, inputPointer, uint32(len(input)))
		if outputPointer != inputPointer {
			_ = t.deallocate(callCtx, outputPointer, outputLength)
		}
		return nil, fmt.Errorf("WebAssembly output is %d bytes, limit is %d", outputLength, t.config.MaxOutputBytes)
	}
	view, ok := t.memory.Read(outputPointer, outputLength)
	if !ok {
		_ = t.deallocate(callCtx, inputPointer, uint32(len(input)))
		if outputPointer != inputPointer {
			_ = t.deallocate(callCtx, outputPointer, outputLength)
		}
		return nil, errors.New("read WebAssembly output outside memory")
	}
	output := append([]byte(nil), view...)
	if err := t.deallocate(callCtx, inputPointer, uint32(len(input))); err != nil {
		return nil, err
	}
	if outputPointer != inputPointer {
		if err := t.deallocate(callCtx, outputPointer, outputLength); err != nil {
			return nil, err
		}
	}
	return output, nil
}

func (t *transformer) deallocate(ctx context.Context, pointer, length uint32) error {
	if _, err := t.dealloc.Call(ctx, uint64(pointer), uint64(length)); err != nil {
		return fmt.Errorf("deallocate WebAssembly memory: %w", err)
	}
	return nil
}
