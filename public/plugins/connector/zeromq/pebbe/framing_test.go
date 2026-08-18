//go:build zeromq_pebbe && cgo

package pebbe

import (
	"errors"
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFujinV1FramingRoundTripsHeadersAndEmptyPayload(t *testing.T) {
	route := routeConfig{name: "events", CommonSettings: CommonSettings{MaxMessageBytes: 1024}, RouteSettings: RouteSettings{Pattern: PatternPub, Framing: FramingFujinV1, Topic: "events."}}
	headers := [][]byte{[]byte("kind"), {0xff}, []byte("kind"), []byte("second")}
	frames, err := encodeMessage(route, nil, headers, true)
	require.NoError(t, err)
	route.Pattern = PatternSub
	decoded, err := decodeMessage(route, frames)
	require.NoError(t, err)
	assert.Empty(t, decoded.payload)
	assert.Equal(t, "events.", decoded.source)
	assert.Equal(t, headers, decoded.headers)
}

func TestRawFramingRejectsHeaders(t *testing.T) {
	route := routeConfig{CommonSettings: CommonSettings{MaxMessageBytes: 1024}, RouteSettings: RouteSettings{Pattern: PatternPush, Framing: FramingRaw}}
	_, err := encodeMessage(route, []byte("payload"), [][]byte{[]byte("k"), []byte("v")}, true)
	require.ErrorIs(t, err, connector.ErrOperationUnsupported)
}

func TestDecodeRejectsMalformedAndOversizedMessages(t *testing.T) {
	route := routeConfig{name: "pull", CommonSettings: CommonSettings{MaxMessageBytes: 1024}, RouteSettings: RouteSettings{Pattern: PatternPull, Framing: FramingFujinV1}}
	_, err := decodeMessage(route, [][]byte{[]byte("wrong"), {0, 0}, nil})
	require.ErrorIs(t, err, errMalformedMessage)
	_, err = decodeMessage(route, [][]byte{fujinV1Magic, {0, 1}, []byte("key"), nil})
	require.ErrorIs(t, err, errMalformedMessage)
	route.MaxMessageBytes = 8
	_, err = decodeMessage(route, [][]byte{fujinV1Magic, {0, 0}, []byte("0123456789")})
	assert.True(t, errors.Is(err, errMessageTooLarge))
}
