package fujin

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/internal/nativeproto"
	"github.com/quic-go/quic-go"
)

var sdkBuild = detectSDKBuildVersion()
var sdkHello = nativeproto.Hello("fujin-go", sdkBuild, nativeproto.Version)

type Conn struct {
	qconn *quic.Conn

	timeout time.Duration
	wdl     time.Duration
	closed  atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	l       *slog.Logger
}

func Dial(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config, opts ...Option) (*Conn, error) {
	if tlsConf == nil {
		tlsConf = &tls.Config{}
	} else {
		tlsConf = tlsConf.Clone()
	}
	tlsConf.NextProtos = []string{nativeproto.Protocol}
	conn, err := quic.DialAddr(ctx, addr, tlsConf, quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}
	controlCtx, cancel := context.WithCancel(context.Background())
	c := &Conn{qconn: conn, timeout: 10 * time.Second, wdl: 5 * time.Second, cancel: cancel, l: slog.Default()}
	for _, option := range opts {
		if option != nil {
			option(c)
		}
	}
	c.wg.Add(1)
	go c.controlLoop(controlCtx)
	return c, nil
}

func (c *Conn) Bind(ctx context.Context, connector string, opts ...client.BindOption) (client.Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	conf := &client.BindConfig{}
	for _, option := range opts {
		if option != nil {
			option(conf)
		}
	}
	stream, err := c.qconn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open session stream: %w", err)
	}
	if c.wdl > 0 {
		_ = stream.SetWriteDeadline(time.Now().Add(c.wdl))
		defer stream.SetWriteDeadline(time.Time{})
	}
	reader := nativeproto.NewReader(stream)
	if _, err := stream.Write(sdkHello); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("write HELLO: %w", err)
	}
	hello, err := reader.HelloResponse()
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("read HELLO: %w", err)
	}
	if c.l.Enabled(ctx, slog.LevelDebug) {
		c.l.Debug("native protocol negotiated", "protocol_version", byte(hello.ProtocolVersion), "server_build", string(hello.ServerBuildVersion))
	}
	if _, err := stream.Write(nativeproto.Bind(connector, conf.Meta, conf.ConfigOverrides)); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("write BIND: %w", err)
	}
	routes, err := reader.BindResponse()
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("read BIND: %w", err)
	}
	return newStream(c, stream, reader, routes), nil
}

func detectSDKBuildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	const modulePath = "github.com/fujin-io/fujin/sdk/go/client"
	if info.Main.Path == modulePath && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	for _, dependency := range info.Deps {
		if dependency.Path != modulePath {
			continue
		}
		if dependency.Replace != nil {
			dependency = dependency.Replace
		}
		if dependency.Version != "" && dependency.Version != "(devel)" {
			return dependency.Version
		}
	}
	return "dev"
}

func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()
	err := c.qconn.CloseWithError(0, "")
	c.wg.Wait()
	if err != nil {
		return fmt.Errorf("close QUIC connection: %w", err)
	}
	return nil
}

func (c *Conn) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || c.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.timeout)
}

func (c *Conn) controlLoop(ctx context.Context) {
	defer c.wg.Done()
	for {
		stream, err := c.qconn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			var request [1]byte
			if _, err := io.ReadFull(stream, request[:]); err != nil || request[0] != nativeproto.OpPing {
				return
			}
			_, _ = stream.Write([]byte{nativeproto.RespPong})
		}()
	}
}
