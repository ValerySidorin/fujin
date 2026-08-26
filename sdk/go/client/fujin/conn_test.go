package fujin_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"testing"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/fujin"
	"github.com/fujin-io/fujin/sdk/go/client/internal/nativeproto"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

type nativeTestServer struct {
	listener  *quic.Listener
	clientTLS *tls.Config
	done      chan error
	ping      chan error
}

func startNativeTestServer(t testing.TB) *nativeTestServer {
	t.Helper()
	serverTLS, clientTLS := nativeTLSConfigs(t)
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{MaxIncomingStreams: 32})
	require.NoError(t, err)
	server := &nativeTestServer{listener: listener, clientTLS: clientTLS, done: make(chan error, 1), ping: make(chan error, 1)}
	go func() { server.done <- server.serve() }()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-server.done:
		case <-time.After(time.Second):
		}
	})
	return server
}

func (s *nativeTestServer) addr() string { return s.listener.Addr().String() }

func (s *nativeTestServer) serve() error {
	ctx := context.Background()
	conn, err := s.listener.Accept(ctx)
	if err != nil {
		return nil
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil
	}
	return serveNativeSession(conn, stream, s.ping)
}

func serveNativeSession(conn *quic.Conn, stream *quic.Stream, pingResult chan<- error) error {
	defer stream.Close()
	reader := bufio.NewReader(stream)
	negotiated := false
	for {
		op, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch op {
		case nativeproto.OpHello:
			format, err := reader.ReadByte()
			if err != nil {
				return err
			}
			if format != nativeproto.HelloFormat {
				return fmt.Errorf("unexpected HELLO format %d", format)
			}
			count, err := reader.ReadByte()
			if err != nil {
				return err
			}
			supported := false
			for range count {
				version, err := reader.ReadByte()
				if err != nil {
					return err
				}
				supported = supported || nativeproto.WireVersion(version) == nativeproto.Version
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if !supported {
				return fmt.Errorf("client does not support protocol version %d", nativeproto.Version)
			}
			if err := writeNative(stream, nativeHelloResponse()); err != nil {
				return err
			}
			negotiated = true
		case nativeproto.OpBind:
			if !negotiated {
				return errors.New("BIND before HELLO")
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if err := discardNativeStringMap(reader); err != nil {
				return err
			}
			if err := discardNativeStringMap(reader); err != nil {
				return err
			}
			if err := writeNative(stream, nativeBindResponse()); err != nil {
				return err
			}
			go pingNativeClient(conn, pingResult)
		case nativeproto.OpProduce, nativeproto.OpHProduce:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			route, err := readNativeString(reader)
			if err != nil {
				return err
			}
			if op == nativeproto.OpHProduce {
				if err := discardNativeHeaders(reader); err != nil {
					return err
				}
			}
			if _, err := readNativeBytes(reader); err != nil {
				return err
			}
			code := nativeproto.RespProduce
			if op == nativeproto.OpHProduce {
				code = nativeproto.RespHProduce
			}
			if route == "missing" {
				if err := writeNative(stream, nativeErrorResponse(code, id)); err != nil {
					return err
				}
			} else if err := writeNative(stream, nativeSuccessResponse(code, id)); err != nil {
				return err
			}
		case nativeproto.OpBeginTx:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if err := writeNative(stream, nativeSuccessResponse(nativeproto.RespBeginTx, id)); err != nil {
				return err
			}
		case nativeproto.OpTxProduce, nativeproto.OpTxHProduce:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			if op == nativeproto.OpTxHProduce {
				if err := discardNativeHeaders(reader); err != nil {
					return err
				}
			}
			if _, err := readNativeBytes(reader); err != nil {
				return err
			}
			code := nativeproto.RespTxProduce
			if op == nativeproto.OpTxHProduce {
				code = nativeproto.RespTxHProduce
			}
			if err := writeNative(stream, nativeSuccessResponse(code, id)); err != nil {
				return err
			}
		case nativeproto.OpCommitTx, nativeproto.OpRollbackTx:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			code := nativeproto.RespCommitTx
			if op == nativeproto.OpRollbackTx {
				code = nativeproto.RespRollbackTx
			}
			if err := writeNative(stream, nativeSuccessResponse(code, id)); err != nil {
				return err
			}
		case nativeproto.OpSubscribe, nativeproto.OpHSubscribe:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			autoSettle, err := reader.ReadByte()
			if err != nil {
				return err
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if err := writeNative(stream, nativeSubscribeResponse(op, id, autoSettle != 0)); err != nil {
				return err
			}
		case nativeproto.OpFetch, nativeproto.OpHFetch:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			autoSettle, err := reader.ReadByte()
			if err != nil {
				return err
			}
			if _, err := readNativeString(reader); err != nil {
				return err
			}
			if _, err := readNativeUint32(reader); err != nil {
				return err
			}
			if err := writeNative(stream, nativeFetchResponse(op, id, autoSettle != 0)); err != nil {
				return err
			}
		case nativeproto.OpAck, nativeproto.OpNack:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			if _, err := reader.ReadByte(); err != nil {
				return err
			}
			count, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			ids := make([][]byte, count)
			for i := range ids {
				ids[i], err = readNativeBytes(reader)
				if err != nil {
					return err
				}
			}
			code := nativeproto.RespAck
			if op == nativeproto.OpNack {
				code = nativeproto.RespNack
			}
			if err := writeNative(stream, nativeSettlementResponse(code, id, ids)); err != nil {
				return err
			}
		case nativeproto.OpUnsubscribe:
			id, err := readNativeUint32(reader)
			if err != nil {
				return err
			}
			if _, err := reader.ReadByte(); err != nil {
				return err
			}
			if err := writeNative(stream, nativeSuccessResponse(nativeproto.RespUnsubscribe, id)); err != nil {
				return err
			}
		case nativeproto.OpDisconnect:
			return writeNative(stream, []byte{nativeproto.RespDisconnect})
		default:
			return fmt.Errorf("unexpected native opcode %d", op)
		}
	}
}

func pingNativeClient(conn *quic.Conn, result chan<- error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err == nil {
		_, err = stream.Write([]byte{nativeproto.OpPing})
	}
	if err == nil {
		var response [1]byte
		_, err = io.ReadFull(stream, response[:])
		if err == nil && response[0] != nativeproto.RespPong {
			err = fmt.Errorf("unexpected pong %d", response[0])
		}
	}
	if stream != nil {
		_ = stream.Close()
	}
	result <- err
}

func nativeTLSConfigs(t testing.TB) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{nativeproto.Protocol}}, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{nativeproto.Protocol}}
}

func nativeHelloResponse() []byte {
	response := []byte{nativeproto.RespHello, 0, nativeproto.HelloFormat, byte(nativeproto.Version)}
	return nativeproto.AppendString(response, "v-server")
}

func nativeBindResponse() []byte {
	response := []byte{nativeproto.RespBind, 0}
	response = binary.BigEndian.AppendUint32(response, 3)
	for _, route := range []struct {
		name                        string
		flags, guarantee, ack, nack byte
	}{{"pub", 3, 1, 0, 0}, {"sub", 58, 0, 1, 3}, {"tx", 7, 1, 0, 0}} {
		response = nativeproto.AppendString(response, route.name)
		response = append(response, route.flags, route.guarantee, route.ack, route.nack)
	}
	return response
}

func nativeSuccessResponse(code byte, id uint32) []byte {
	response := []byte{code}
	response = binary.BigEndian.AppendUint32(response, id)
	return append(response, 0)
}

func nativeErrorResponse(code byte, id uint32) []byte {
	response := []byte{code}
	response = binary.BigEndian.AppendUint32(response, id)
	response = append(response, byte(client.StatusNotFound), byte(client.OutcomeNotApplied))
	response = nativeproto.AppendString(response, "ROUTE_NOT_FOUND")
	response = nativeproto.AppendString(response, "route not found")
	return binary.BigEndian.AppendUint16(response, 0)
}

func nativeSubscribeResponse(op byte, id uint32, autoSettle bool) []byte {
	responseCode, messageCode := nativeproto.RespSubscribe, nativeproto.RespMessage
	if op == nativeproto.OpHSubscribe {
		responseCode, messageCode = nativeproto.RespHSubscribe, nativeproto.RespHMessage
	}
	response := nativeSuccessResponse(responseCode, id)
	response = append(response, 7, messageCode, 7)
	if op == nativeproto.OpHSubscribe {
		response = appendNativeHeaders(response, []client.Header{{Key: []byte("content-type"), Value: []byte("text/plain")}})
	}
	if !autoSettle {
		response = nativeproto.AppendBytes(response, []byte("message-id"))
	}
	return nativeproto.AppendBytes(response, []byte("subscription-message"))
}

func nativeFetchResponse(op byte, id uint32, autoSettle bool) []byte {
	code := nativeproto.RespFetch
	if op == nativeproto.OpHFetch {
		code = nativeproto.RespHFetch
	}
	response := nativeSuccessResponse(code, id)
	response = append(response, 9)
	response = binary.BigEndian.AppendUint32(response, 1)
	if op == nativeproto.OpHFetch {
		response = appendNativeHeaders(response, []client.Header{{Key: []byte("content-type"), Value: []byte("application/octet-stream")}})
	}
	if !autoSettle {
		response = nativeproto.AppendBytes(response, []byte("fetch-id"))
	}
	return nativeproto.AppendBytes(response, []byte("fetched-message"))
}

func nativeSettlementResponse(code byte, id uint32, ids [][]byte) []byte {
	response := nativeSuccessResponse(code, id)
	response = binary.BigEndian.AppendUint32(response, uint32(len(ids)))
	for _, messageID := range ids {
		response = nativeproto.AppendBytes(response, messageID)
		response = append(response, 0)
	}
	return response
}

func appendNativeHeaders(dst []byte, headers []client.Header) []byte {
	return nativeproto.AppendHeaders(dst, headers)
}
func writeNative(w io.Writer, payload []byte) error { _, err := w.Write(payload); return err }

func readNativeUint32(r io.Reader) (uint32, error) {
	var value [4]byte
	_, err := io.ReadFull(r, value[:])
	return binary.BigEndian.Uint32(value[:]), err
}
func readNativeBytes(r io.Reader) ([]byte, error) {
	size, err := readNativeUint32(r)
	if err != nil {
		return nil, err
	}
	value := make([]byte, size)
	_, err = io.ReadFull(r, value)
	return value, err
}
func readNativeString(r io.Reader) (string, error) {
	value, err := readNativeBytes(r)
	return string(value), err
}
func discardNativeStringMap(r io.Reader) error {
	var count [2]byte
	if _, err := io.ReadFull(r, count[:]); err != nil {
		return err
	}
	for range binary.BigEndian.Uint16(count[:]) {
		if _, err := readNativeString(r); err != nil {
			return err
		}
		if _, err := readNativeString(r); err != nil {
			return err
		}
	}
	return nil
}
func discardNativeHeaders(r io.Reader) error {
	var count [2]byte
	if _, err := io.ReadFull(r, count[:]); err != nil {
		return err
	}
	for range binary.BigEndian.Uint16(count[:]) {
		if _, err := readNativeBytes(r); err != nil {
			return err
		}
	}
	return nil
}

func TestConnCloseIsIdempotent(t *testing.T) {
	server := startNativeTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := fujin.Dial(ctx, server.addr(), server.clientTLS, nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Close())
}
