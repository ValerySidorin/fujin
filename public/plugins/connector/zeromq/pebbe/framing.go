//go:build zeromq_pebbe && cgo

package pebbe

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

var (
	errMalformedMessage = errors.New("malformed ZeroMQ message")
	errMessageTooLarge  = errors.New("ZeroMQ message exceeds max_message_bytes")
)

var fujinV1Magic = []byte("fujin.v1")

type decodedMessage struct {
	payload []byte
	source  string
	headers [][]byte
}

func encodeMessage(route routeConfig, payload []byte, headers [][]byte, withHeaders bool) ([][]byte, error) {
	if len(payload) > route.MaxMessageBytes {
		return nil, errMessageTooLarge
	}
	if route.Framing == FramingRaw {
		if withHeaders {
			return nil, connector.ErrOperationUnsupported
		}
		var frames [][]byte
		if route.Pattern == PatternPub {
			frames = [][]byte{[]byte(route.Topic), payload}
		} else {
			frames = [][]byte{payload}
		}
		if messageSize(frames) > route.MaxMessageBytes {
			return nil, errMessageTooLarge
		}
		return frames, nil
	}
	if err := connector.ValidateHeaders(headers); err != nil {
		return nil, err
	}
	if len(headers) > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: too many header fields", errMalformedMessage)
	}
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(headers)))
	capacity := 3 + len(headers)
	if route.Pattern == PatternPub {
		capacity++
	}
	frames := make([][]byte, 0, capacity)
	if route.Pattern == PatternPub {
		frames = append(frames, []byte(route.Topic))
	}
	frames = append(frames, fujinV1Magic, count[:])
	frames = append(frames, headers...)
	frames = append(frames, payload)
	if messageSize(frames) > route.MaxMessageBytes {
		return nil, errMessageTooLarge
	}
	return frames, nil
}

func decodeMessage(route routeConfig, frames [][]byte) (decodedMessage, error) {
	if messageSize(frames) > route.MaxMessageBytes {
		return decodedMessage{}, errMessageTooLarge
	}
	topicFrames := 0
	source := route.name
	if route.Pattern == PatternSub {
		topicFrames = 1
		if len(frames) > 0 {
			source = string(frames[0])
		}
	}
	if route.Framing == FramingRaw {
		expected := 1 + topicFrames
		if len(frames) != expected {
			return decodedMessage{}, fmt.Errorf("%w: expected %d frames, got %d", errMalformedMessage, expected, len(frames))
		}
		return decodedMessage{payload: frames[len(frames)-1], source: source}, nil
	}
	if len(frames) < topicFrames+3 || string(frames[topicFrames]) != string(fujinV1Magic) || len(frames[topicFrames+1]) != 2 {
		return decodedMessage{}, errMalformedMessage
	}
	count := int(binary.BigEndian.Uint16(frames[topicFrames+1]))
	if count%2 != 0 || len(frames) != topicFrames+3+count {
		return decodedMessage{}, errMalformedMessage
	}
	headers := frames[topicFrames+2 : topicFrames+2+count]
	if err := connector.ValidateHeaders(headers); err != nil {
		return decodedMessage{}, fmt.Errorf("%w: %v", errMalformedMessage, err)
	}
	return decodedMessage{payload: frames[len(frames)-1], source: source, headers: headers}, nil
}

func messageSize(frames [][]byte) int {
	total := 0
	for _, frame := range frames {
		total += len(frame)
	}
	return total
}
