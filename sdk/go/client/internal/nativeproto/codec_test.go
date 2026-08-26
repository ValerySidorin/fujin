package nativeproto

import (
	"bytes"
	"encoding/binary"
	"testing"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/stretchr/testify/require"
)

func TestHelloFrameCarriesVersionsAndClientBuild(t *testing.T) {
	frame := Hello("fujin-go", "v1.2.3", Version)
	want := []byte{OpHello, HelloFormat, 1, byte(Version)}
	want = AppendString(want, "fujin-go")
	want = AppendString(want, "v1.2.3")
	require.Equal(t, want, frame)
}

func TestHelloResponseReturnsNegotiatedServerInfo(t *testing.T) {
	response := []byte{RespHello, 0, HelloFormat, byte(Version)}
	response = AppendString(response, "v0.5.0")

	info, err := NewReader(bytes.NewReader(response)).HelloResponse()
	require.NoError(t, err)
	require.Equal(t, Version, info.ProtocolVersion)
	require.Equal(t, []byte("v0.5.0"), info.ServerBuildVersion)
}

func TestProducePrefixMatchesCompleteFrame(t *testing.T) {
	headers := []client.Header{
		{Key: []byte("content-type"), Value: []byte("application/octet-stream")},
		{Key: []byte("tenant"), Value: []byte("test")},
	}
	payload := []byte("payload")
	prefix := ProducePrefix(OpHProduce, 42, "pub", len(payload), headers)
	frame := Produce(OpHProduce, 42, "pub", payload, headers)

	require.Equal(t, append(append([]byte(nil), prefix...), payload...), frame)
	require.Equal(t, uint32(len(payload)), binary.BigEndian.Uint32(prefix[len(prefix)-4:]))
	require.Equal(t, len(prefix), cap(prefix))
}

func TestProducePrefixOmitsRouteForTransactionalProduce(t *testing.T) {
	payload := []byte("transactional")
	prefix := ProducePrefix(OpTxProduce, 7, "ignored", len(payload), nil)
	frame := Produce(OpTxProduce, 7, "ignored", payload, nil)

	require.Equal(t, append(append([]byte(nil), prefix...), payload...), frame)
	require.Equal(t, 9, len(prefix))
}
