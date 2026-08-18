package test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("nop", nopDescriptor()); err != nil {
		panic(fmt.Sprintf("register nop connector: %v", err))
	}
}

func nopDescriptor() connector.Descriptor {
	return connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
		profiles := map[string]connector.RouteProfile{
			"pub": {
				Produce:          true,
				Headers:          true,
				Transactions:     true,
				ProduceGuarantee: connector.AcceptanceLocal,
			},
			"sub": {
				Headers:          true,
				Subscribe:        true,
				Fetch:            true,
				ManualSettlement: true,
				Settlement:       connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackDrop},
			},
		}
		factories := map[string]connector.RouteFactory{
			"pub": {Writer: func(*slog.Logger) (connector.WriteCloser, error) { return newWriter(), nil }},
			"sub": {Reader: func(bool, *slog.Logger) (connector.ReadCloser, error) { return newReader(), nil }},
		}
		return connector.CompileStatic(profiles, factories)
	}}
}

type reader struct{}

func newReader() connector.ReadCloser { return &reader{} }

func (*reader) Subscribe(ctx context.Context, ready func() error, _ func([]byte, string, ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (*reader) SubscribeWithHeaders(ctx context.Context, ready func() error, _ func([]byte, string, [][]byte, ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (*reader) Fetch(_ context.Context, _ uint32, done func(uint32, error), _ func([]byte, string, ...any)) {
	done(0, nil)
}

func (*reader) FetchWithHeaders(_ context.Context, _ uint32, done func(uint32, error), _ func([]byte, string, [][]byte, ...any)) {
	done(0, nil)
}

func (*reader) Ack(_ context.Context, ids [][]byte, done func(error), each func([]byte, error)) {
	done(nil)
	for _, id := range ids {
		each(id, nil)
	}
}

func (*reader) Nack(_ context.Context, ids [][]byte, done func(error), each func([]byte, error)) {
	done(nil)
	for _, id := range ids {
		each(id, nil)
	}
}

func (*reader) EncodeMsgID(buf []byte, _ string, _ ...any) []byte { return buf }
func (*reader) MsgIDArgsLen() int                                 { return 0 }
func (*reader) AutoCommit() bool                                  { return true }
func (*reader) Close() error                                      { return nil }

type writer struct{}

func newWriter() connector.WriteCloser { return &writer{} }
func (*writer) Produce(_ context.Context, _ []byte, callback func(error)) {
	callback(nil)
}
func (*writer) HProduce(_ context.Context, _ []byte, _ [][]byte, callback func(error)) {
	callback(nil)
}
func (*writer) Flush(context.Context) error      { return nil }
func (*writer) BeginTx(context.Context) error    { return nil }
func (*writer) CommitTx(context.Context) error   { return nil }
func (*writer) RollbackTx(context.Context) error { return nil }
func (*writer) Close() error                     { return nil }

func (*writer) WriterContractCompliant() {}
