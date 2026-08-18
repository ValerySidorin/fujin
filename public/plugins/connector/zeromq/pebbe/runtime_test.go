//go:build zeromq_pebbe && cgo

package pebbe

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestRuntime(t *testing.T, route RouteSettings, mutate func(*CommonSettings)) *runtime {
	t.Helper()
	common := CommonSettings{}
	applyCommonDefaults(&common)
	if mutate != nil {
		mutate(&common)
	}
	config := Config{Common: common, Routes: map[string]RouteSettings{"route": route}}
	require.NoError(t, config.normalizeAndValidate())
	opened, err := openRuntime(config, slog.Default())
	require.NoError(t, err)
	r := opened.(*runtime)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, r.Close(ctx))
	})
	return r
}

func newPeerContext(t *testing.T) *zmq.Context {
	t.Helper()
	ctx, err := zmq.NewContext()
	require.NoError(t, err)
	ctx.SetRetryAfterEINTR(true)
	t.Cleanup(func() { require.NoError(t, ctx.Term()) })
	return ctx
}

func routeEndpoint(t *testing.T, route *routeRuntime) string {
	t.Helper()
	var socket *zmq.Socket
	if route.writer != nil {
		socket = route.writer.socket
	} else {
		socket = route.reader.socket
	}
	endpoint, err := socket.GetLastEndpoint()
	require.NoError(t, err)
	return endpoint
}

func TestWriterRoutesInteroperateWithIndependentZeroMQPeers(t *testing.T) {
	for _, pattern := range []string{PatternPub, PatternPush} {
		t.Run(pattern, func(t *testing.T) {
			route := RouteSettings{Pattern: pattern, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingFujinV1}
			peerType := zmq.PULL
			if pattern == PatternPub {
				route.Topic = "events."
				peerType = zmq.SUB
			}
			r := openTestRuntime(t, route, nil)
			peer, err := newPeerContext(t).NewSocket(peerType)
			require.NoError(t, err)
			require.NoError(t, peer.SetLinger(0))
			require.NoError(t, peer.SetRcvtimeo(2*time.Second))
			if pattern == PatternPub {
				require.NoError(t, peer.SetSubscribe("events."))
			}
			require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			time.Sleep(200 * time.Millisecond)

			underlying, err := r.NewWriter("route", slog.Default())
			require.NoError(t, err)
			writer := connector.EnforceWriterContract(underlying)
			callback := make(chan error, 1)
			writer.HProduce(context.Background(), []byte("payload"), [][]byte{[]byte("kind"), []byte("test")}, func(err error) { callback <- err })
			require.NoError(t, <-callback)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, writer.Flush(ctx))
			frames, err := peer.RecvMessageBytes(0)
			require.NoError(t, err)
			if pattern == PatternPub {
				assert.Equal(t, []byte("events."), frames[0])
				frames = frames[1:]
			}
			assert.Equal(t, [][]byte{fujinV1Magic, {0, 2}, []byte("kind"), []byte("test"), []byte("payload")}, frames)
			require.NoError(t, writer.Close())
		})
	}
}

func TestWriterContractCallbacksFlushAndClose(t *testing.T) {
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPush, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingRaw}, nil)
	peer, err := newPeerContext(t).NewSocket(zmq.PULL)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	underlying, err := r.NewWriter("route", slog.Default())
	require.NoError(t, err)
	writer := connector.EnforceWriterContract(underlying)
	callbacks := make(chan int, 3)
	for index := 0; index < 3; index++ {
		index := index
		writer.Produce(context.Background(), []byte{byte(index)}, func(err error) {
			require.NoError(t, err)
			callbacks <- index
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, writer.Flush(ctx))
	seen := [3]int{}
	for index := 0; index < 3; index++ {
		seen[<-callbacks]++
	}
	assert.Equal(t, [3]int{1, 1, 1}, seen)
	require.NoError(t, writer.Close())
	closedCallback := make(chan error, 1)
	writer.Produce(context.Background(), nil, func(err error) { closedCallback <- err })
	require.ErrorIs(t, <-closedCallback, connector.ErrWriterClosed)
}

func TestWriterSendTimeoutIsReported(t *testing.T) {
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPush, Endpoint: freeTCPEndpoint(t), Mode: ModeConnect, Framing: FramingRaw}, func(common *CommonSettings) {
		common.SendTimeout = 50 * time.Millisecond
		common.SendHWM = 1
	})
	writer, err := r.NewWriter("route", slog.Default())
	require.NoError(t, err)
	callback := make(chan error, 1)
	writer.Produce(context.Background(), []byte("timeout"), func(err error) { callback <- err })
	select {
	case err := <-callback:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("send timeout callback did not resolve")
	}
	require.NoError(t, writer.Close())
}

func TestConnectOwnedWriterAndReaderRoutes(t *testing.T) {
	t.Run("PUB connect", func(t *testing.T) {
		endpoint := freeTCPEndpoint(t)
		peer, err := newPeerContext(t).NewSocket(zmq.SUB)
		require.NoError(t, err)
		require.NoError(t, peer.SetLinger(0))
		require.NoError(t, peer.SetRcvtimeo(100*time.Millisecond))
		require.NoError(t, peer.SetSubscribe("events."))
		require.NoError(t, peer.Bind(endpoint))
		t.Cleanup(func() { require.NoError(t, peer.Close()) })
		r := openTestRuntime(t, RouteSettings{Pattern: PatternPub, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingRaw, Topic: "events."}, nil)
		select {
		case <-r.routes["route"].writer.ready:
		case <-time.After(2 * time.Second):
			t.Fatal("PUB connect handshake not ready")
		}
		writer, err := r.NewWriter("route", slog.Default())
		require.NoError(t, err)
		var frames [][]byte
		require.Eventually(t, func() bool {
			callback := make(chan error, 1)
			writer.Produce(context.Background(), []byte("connected"), func(err error) { callback <- err })
			if <-callback != nil {
				return false
			}
			frames, err = peer.RecvMessageBytes(0)
			return err == nil
		}, 2*time.Second, 20*time.Millisecond)
		assert.Equal(t, [][]byte{[]byte("events."), []byte("connected")}, frames)
		require.NoError(t, writer.Close())
	})

	t.Run("PULL connect", func(t *testing.T) {
		endpoint := freeTCPEndpoint(t)
		peer, err := newPeerContext(t).NewSocket(zmq.PUSH)
		require.NoError(t, err)
		require.NoError(t, peer.SetLinger(0))
		require.NoError(t, peer.SetSndtimeo(2*time.Second))
		require.NoError(t, peer.Bind(endpoint))
		t.Cleanup(func() { require.NoError(t, peer.Close()) })
		r := openTestRuntime(t, RouteSettings{Pattern: PatternPull, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingRaw}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader, err := r.NewReader("route", true, slog.Default())
		require.NoError(t, err)
		ready := make(chan struct{})
		message := make(chan string, 1)
		subscribeErr := make(chan error, 1)
		go func() {
			subscribeErr <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func(payload []byte, _ string, _ ...any) { message <- string(payload) })
		}()
		select {
		case <-ready:
		case <-time.After(2 * time.Second):
			t.Fatal("connect reader not ready")
		}
		require.NoError(t, sendFrames(peer, [][]byte{[]byte("connected")}))
		select {
		case actual := <-message:
			assert.Equal(t, "connected", actual)
		case <-time.After(2 * time.Second):
			t.Fatal("connect reader did not receive")
		}
		cancel()
		require.NoError(t, <-subscribeErr)
	})
}

func TestIPCWriterRoute(t *testing.T) {
	endpoint := "ipc://" + filepath.Join(t.TempDir(), "zeromq.sock")
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPush, Endpoint: endpoint, Mode: ModeBind, Framing: FramingRaw}, nil)
	peer, err := newPeerContext(t).NewSocket(zmq.PULL)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.SetRcvtimeo(2*time.Second))
	require.NoError(t, peer.Connect(endpoint))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	writer, err := r.NewWriter("route", slog.Default())
	require.NoError(t, err)
	callback := make(chan error, 1)
	writer.Produce(context.Background(), []byte("ipc"), func(err error) { callback <- err })
	require.NoError(t, <-callback)
	frames, err := peer.RecvMessageBytes(0)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("ipc")}, frames)
	require.NoError(t, writer.Close())
}

func TestMalformedMessagesAreCountedAndDoNotStopRoute(t *testing.T) {
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingFujinV1}, func(common *CommonSettings) { common.MaxMessageBytes = 64 })
	peer, err := newPeerContext(t).NewSocket(zmq.PUSH)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))

	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := r.NewReader("route", true, slog.Default())
	require.NoError(t, err)
	ready := make(chan struct{})
	delivered := make(chan string, 1)
	subscribeErr := make(chan error, 1)
	go func() {
		subscribeErr <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func(payload []byte, _ string, _ ...any) { delivered <- string(payload) })
	}()
	<-ready
	require.NoError(t, sendFrames(peer, [][]byte{[]byte("bad")}))
	require.NoError(t, sendFrames(peer, [][]byte{fujinV1Magic, {0, 0}, make([]byte, 128)}))
	require.Eventually(t, func() bool {
		return r.routes["route"].reader.malformed.Load() == 1 && r.routes["route"].reader.oversized.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, sendFrames(peer, [][]byte{fujinV1Magic, {0, 0}, []byte("valid")}))
	select {
	case actual := <-delivered:
		assert.Equal(t, "valid", actual)
	case <-time.After(2 * time.Second):
		t.Fatal("route stopped after malformed input")
	}
	cancel()
	require.NoError(t, <-subscribeErr)
}
func TestCatalogReloadReusesBindRuntimeAndRequiresDrain(t *testing.T) {
	endpoint := freeTCPEndpoint(t)
	oldConfig := connectorconfig.ConnectorsConfig{"connector": {Type: connectorName, Settings: Config{Routes: map[string]RouteSettings{
		"route": {Pattern: PatternPush, Endpoint: endpoint, Mode: ModeBind, Framing: FramingRaw},
	}}}}
	newConfig := connectorconfig.ConnectorsConfig{"connector": {Type: connectorName, Settings: Config{Routes: map[string]RouteSettings{
		"route": {Pattern: PatternPush, Endpoint: endpoint, Mode: ModeBind, Framing: FramingFujinV1},
	}}}}
	catalog, err := connector.CompileCatalog(oldConfig, slog.Default())
	require.NoError(t, err)
	require.NoError(t, catalog.Reload(oldConfig))
	active := catalog.Current()
	err = catalog.Reload(newConfig)
	require.ErrorIs(t, err, connector.ErrRuntimeDrainRequired)
	assert.Same(t, active, catalog.Current())
	require.NoError(t, catalog.Reload(connectorconfig.ConnectorsConfig{}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, active.WaitClosed(ctx))
	require.NoError(t, catalog.Reload(newConfig))
	require.NoError(t, catalog.Close(ctx))
}

func TestConnectReaderReconnectsAfterPeerRestart(t *testing.T) {
	endpoint := freeTCPEndpoint(t)
	peerContext := newPeerContext(t)
	openPeer := func() *zmq.Socket {
		peer, err := peerContext.NewSocket(zmq.PUSH)
		require.NoError(t, err)
		require.NoError(t, peer.SetLinger(0))
		require.NoError(t, peer.SetSndtimeo(2*time.Second))
		require.NoError(t, peer.Bind(endpoint))
		return peer
	}
	peer := openPeer()
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPull, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingRaw}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := r.NewReader("route", true, slog.Default())
	require.NoError(t, err)
	ready := make(chan struct{})
	delivered := make(chan string, 2)
	subscribeErr := make(chan error, 1)
	go func() {
		subscribeErr <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func(payload []byte, _ string, _ ...any) { delivered <- string(payload) })
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("connect reader not ready")
	}
	require.NoError(t, sendFrames(peer, [][]byte{[]byte("before")}))
	select {
	case actual := <-delivered:
		assert.Equal(t, "before", actual)
	case <-time.After(2 * time.Second):
		t.Fatal("message before restart not delivered")
	}
	require.NoError(t, peer.Close())
	time.Sleep(150 * time.Millisecond)
	peer = openPeer()
	defer peer.Close()
	require.Eventually(t, func() bool {
		if err := sendFrames(peer, [][]byte{[]byte("after")}); err != nil {
			return false
		}
		select {
		case actual := <-delivered:
			return actual == "after"
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}, 5*time.Second, 100*time.Millisecond)
	cancel()
	require.NoError(t, <-subscribeErr)
}

func TestRuntimeShutdownIsBoundedWithoutConnectedPeer(t *testing.T) {
	common := CommonSettings{}
	applyCommonDefaults(&common)
	config := Config{Common: common, Routes: map[string]RouteSettings{"route": {
		Pattern: PatternPush, Endpoint: freeTCPEndpoint(t), Mode: ModeConnect, Framing: FramingRaw,
	}}}
	require.NoError(t, config.normalizeAndValidate())
	opened, err := openRuntime(config, slog.Default())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, opened.Close(ctx))
}

func TestRuntimeRepeatedOpenCloseDoesNotLeakGoroutines(t *testing.T) {
	baseline := goruntime.NumGoroutine()
	for iteration := 0; iteration < 20; iteration++ {
		common := CommonSettings{}
		applyCommonDefaults(&common)
		common.ReceivePollInterval = 10 * time.Millisecond
		config := Config{Common: common, Routes: map[string]RouteSettings{"route": {
			Pattern: PatternPush, Endpoint: freeTCPEndpoint(t), Mode: ModeConnect, Framing: FramingRaw,
		}}}
		require.NoError(t, config.normalizeAndValidate())
		opened, err := openRuntime(config, slog.Default())
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, opened.Close(ctx))
		cancel()
	}
	goruntime.GC()
	require.Eventually(t, func() bool { return goruntime.NumGoroutine() <= baseline+8 }, 2*time.Second, 20*time.Millisecond)
}

func TestPullDistributesMessagesAcrossLocalReaders(t *testing.T) {
	r := openTestRuntime(t, RouteSettings{Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingRaw}, nil)
	peer, err := newPeerContext(t).NewSocket(zmq.PUSH)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.SetSndtimeo(time.Second))
	require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan int, 20)
	errs := make(chan error, 2)
	for index := 0; index < 2; index++ {
		reader, err := r.NewReader("route", true, slog.Default())
		require.NoError(t, err)
		id := index
		ready := make(chan struct{})
		go func() {
			errs <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func([]byte, string, ...any) { results <- id })
		}()
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("reader not ready")
		}
	}
	time.Sleep(100 * time.Millisecond)
	for index := 0; index < 20; index++ {
		require.NoError(t, sendFrames(peer, [][]byte{[]byte(fmt.Sprintf("%d", index))}))
	}
	counts := [2]int{}
	for index := 0; index < 20; index++ {
		select {
		case id := <-results:
			counts[id]++
		case <-time.After(2 * time.Second):
			t.Fatal("message not delivered")
		}
	}
	assert.Positive(t, counts[0])
	assert.Positive(t, counts[1])
	cancel()
	for index := 0; index < 2; index++ {
		require.NoError(t, <-errs)
	}
}

func TestSubDetachesSlowConsumerWithoutBlockingPeers(t *testing.T) {
	r := openTestRuntime(t, RouteSettings{Pattern: PatternSub, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingRaw, Subscriptions: []string{"events."}}, func(common *CommonSettings) { common.SubscriberQueueCapacity = 1 })
	peer, err := newPeerContext(t).NewSocket(zmq.PUB)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow, err := r.NewReader("route", true, slog.Default())
	require.NoError(t, err)
	fast, err := r.NewReader("route", true, slog.Default())
	require.NoError(t, err)
	slowReady, fastReady := make(chan struct{}), make(chan struct{})
	slowStarted, unblockSlow := make(chan struct{}), make(chan struct{})
	var slowStartedOnce sync.Once
	slowErr, fastErr := make(chan error, 1), make(chan error, 1)
	fastMessages := make(chan string, 3)
	go func() {
		slowErr <- slow.Subscribe(ctx, func() error { close(slowReady); return nil }, func([]byte, string, ...any) { slowStartedOnce.Do(func() { close(slowStarted) }); <-unblockSlow })
	}()
	go func() {
		fastErr <- fast.Subscribe(ctx, func() error { close(fastReady); return nil }, func(message []byte, _ string, _ ...any) { fastMessages <- string(message) })
	}()
	<-slowReady
	<-fastReady
	time.Sleep(250 * time.Millisecond)
	require.NoError(t, sendFrames(peer, [][]byte{[]byte("events."), []byte("one")}))
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber did not receive first message")
	}
	for _, payload := range []string{"two", "three"} {
		require.NoError(t, sendFrames(peer, [][]byte{[]byte("events."), []byte(payload)}))
	}
	for _, expected := range []string{"one", "two", "three"} {
		select {
		case actual := <-fastMessages:
			assert.Equal(t, expected, actual)
		case <-time.After(2 * time.Second):
			t.Fatal("fast subscriber stalled")
		}
	}
	close(unblockSlow)
	select {
	case err := <-slowErr:
		require.ErrorIs(t, err, ErrSlowConsumer)
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber was not detached")
	}
	cancel()
	require.NoError(t, <-fastErr)
}

func TestContextScopedZAPAllowsConfiguredCurveClient(t *testing.T) {
	clientPublic, clientSecret, err := zmq.NewCurveKeypair()
	require.NoError(t, err)
	serverPublic, serverSecret, err := zmq.NewCurveKeypair()
	require.NoError(t, err)
	secretPath := filepath.Join(t.TempDir(), "server.key")
	require.NoError(t, os.WriteFile(secretPath, []byte(serverSecret+"\n"), 0o600))
	route := RouteSettings{Pattern: PatternPull, Endpoint: "tcp://127.0.0.1:*", Mode: ModeBind, Framing: FramingRaw, Security: SecuritySettings{Mechanism: SecurityCurve, PublicKey: serverPublic, SecretKeyPath: secretPath, AllowedClientPublicKeys: []string{clientPublic}}}
	r := openTestRuntime(t, route, nil)
	peer, err := newPeerContext(t).NewSocket(zmq.PUSH)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.SetSndtimeo(2*time.Second))
	require.NoError(t, peer.SetCurveServerkey(serverPublic))
	require.NoError(t, peer.SetCurvePublickey(clientPublic))
	require.NoError(t, peer.SetCurveSecretkey(clientSecret))
	require.NoError(t, peer.Connect(routeEndpoint(t, r.routes["route"])))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	reader, err := r.NewReader("route", true, slog.Default())
	require.NoError(t, err)
	ready := make(chan struct{})
	message := make(chan string, 1)
	subscribeErr := make(chan error, 1)
	go func() {
		subscribeErr <- reader.Subscribe(ctx, func() error { close(ready); return nil }, func(payload []byte, _ string, _ ...any) { message <- string(payload) })
	}()
	<-ready
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, sendFrames(peer, [][]byte{[]byte("secured")}))
	select {
	case actual := <-message:
		assert.Equal(t, "secured", actual)
	case <-time.After(3 * time.Second):
		t.Fatal("CURVE message not delivered")
	}
	cancel()
	require.NoError(t, <-subscribeErr)
}

func TestCurveConnectRouteAuthenticatesToServer(t *testing.T) {
	clientPublic, clientSecret, err := zmq.NewCurveKeypair()
	require.NoError(t, err)
	serverPublic, serverSecret, err := zmq.NewCurveKeypair()
	require.NoError(t, err)
	endpoint := freeTCPEndpoint(t)
	peer, err := newPeerContext(t).NewSocket(zmq.PULL)
	require.NoError(t, err)
	require.NoError(t, peer.SetLinger(0))
	require.NoError(t, peer.SetRcvtimeo(2*time.Second))
	require.NoError(t, peer.SetCurveServer(1))
	require.NoError(t, peer.SetCurveSecretkey(serverSecret))
	require.NoError(t, peer.Bind(endpoint))
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	secretPath := filepath.Join(t.TempDir(), "client.key")
	require.NoError(t, os.WriteFile(secretPath, []byte(clientSecret), 0o600))
	route := RouteSettings{Pattern: PatternPush, Endpoint: endpoint, Mode: ModeConnect, Framing: FramingRaw, Security: SecuritySettings{
		Mechanism: SecurityCurve, PublicKey: clientPublic, SecretKeyPath: secretPath, ServerPublicKey: serverPublic,
	}}
	r := openTestRuntime(t, route, nil)
	select {
	case <-r.routes["route"].writer.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("CURVE client handshake not ready")
	}
	writer, err := r.NewWriter("route", slog.Default())
	require.NoError(t, err)
	callback := make(chan error, 1)
	writer.Produce(context.Background(), []byte("secured"), func(err error) { callback <- err })
	require.NoError(t, <-callback)
	frames, err := peer.RecvMessageBytes(0)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("secured")}, frames)
	require.NoError(t, writer.Close())
}

func TestZAPResponseRejectsUnknownClient(t *testing.T) {
	knownRaw := string(make([]byte, 32))
	known := zmq.Z85encode(knownRaw)
	unknown := make([]byte, 32)
	unknown[0] = 1
	request := [][]byte{[]byte("1.0"), []byte("1"), []byte("domain"), nil, nil, []byte("CURVE"), unknown}
	response := zapResponse(request, map[string]map[string]struct{}{"domain": {known: {}}})
	assert.Equal(t, []byte("400"), response[2])
	request[6] = []byte(knownRaw)
	response = zapResponse(request, map[string]map[string]struct{}{"domain": {known: {}}})
	assert.Equal(t, []byte("200"), response[2])
}
