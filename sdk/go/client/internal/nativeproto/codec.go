package nativeproto

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	fujin "github.com/fujin-io/fujin/sdk/go/client"
)

type WireVersion byte

const (
	Protocol                = "fujin"
	Version     WireVersion = 1
	HelloFormat             = 1
)

const OpHello byte = 0

const (
	OpBind byte = 1 + iota
	OpProduce
	OpHProduce
	OpBeginTx
	OpCommitTx
	OpRollbackTx
	OpFetch
	OpHFetch
	OpAck
	OpNack
	OpSubscribe
	OpHSubscribe
	OpUnsubscribe
	OpDisconnect
	OpTxProduce
	OpTxHProduce
)

const (
	RespSubscribe byte = 1 + iota
	RespHSubscribe
	RespProduce
	RespHProduce
	RespBeginTx
	RespCommitTx
	RespRollbackTx
	RespMessage
	RespHMessage
	RespFetch
	RespHFetch
	RespAck
	RespNack
	RespUnsubscribe
	RespDisconnect
	RespBind
	RespTxProduce
	RespTxHProduce
	RespHello byte = 19
)

const (
	OpStop   byte = 98
	OpPing   byte = 99
	RespPong byte = 99
)

func AppendString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

func AppendBytes(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

func AppendStringMap(dst []byte, values map[string]string) []byte {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(keys)))
	for _, key := range keys {
		dst = AppendString(dst, key)
		dst = AppendString(dst, values[key])
	}
	return dst
}

func AppendHeaders(dst []byte, headers []fujin.Header) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(headers)*2))
	for _, header := range headers {
		dst = AppendBytes(dst, header.Key)
		dst = AppendBytes(dst, header.Value)
	}
	return dst
}

func headersSize(headers []fujin.Header) int {
	size := 2
	for _, header := range headers {
		size += 8 + len(header.Key) + len(header.Value)
	}
	return size
}

func Hello(clientName, clientBuild string, versions ...WireVersion) []byte {
	frame := make([]byte, 0, 3+len(versions)+2*4+len(clientName)+len(clientBuild))
	frame = append(frame, OpHello, HelloFormat, byte(len(versions)))
	for _, version := range versions {
		frame = append(frame, byte(version))
	}
	frame = AppendString(frame, clientName)
	return AppendString(frame, clientBuild)
}

type HelloInfo struct {
	ProtocolVersion    WireVersion
	ServerBuildVersion []byte
}

func Bind(connector string, meta, overrides map[string]string) []byte {
	frame := make([]byte, 0, 1+4+len(connector)+4)
	frame = append(frame, OpBind)
	frame = AppendString(frame, connector)
	frame = AppendStringMap(frame, meta)
	return AppendStringMap(frame, overrides)
}

func ProducePrefix(op byte, id uint32, route string, payloadSize int, headers []fujin.Header) []byte {
	frame := make([]byte, 0, producePrefixSize(op, route, headers))
	return appendProducePrefix(frame, op, id, route, payloadSize, headers)
}

func Produce(op byte, id uint32, route string, payload []byte, headers []fujin.Header) []byte {
	frame := make([]byte, 0, producePrefixSize(op, route, headers)+len(payload))
	frame = appendProducePrefix(frame, op, id, route, len(payload), headers)
	return append(frame, payload...)
}

func producePrefixSize(op byte, route string, headers []fujin.Header) int {
	size := 1 + 4 + 4
	if op == OpProduce || op == OpHProduce {
		size += 4 + len(route)
	}
	if op == OpHProduce || op == OpTxHProduce {
		size += headersSize(headers)
	}
	return size
}

func appendProducePrefix(frame []byte, op byte, id uint32, route string, payloadSize int, headers []fujin.Header) []byte {
	frame = append(frame, op)
	frame = binary.BigEndian.AppendUint32(frame, id)
	if op == OpProduce || op == OpHProduce {
		frame = AppendString(frame, route)
	}
	if op == OpHProduce || op == OpTxHProduce {
		frame = AppendHeaders(frame, headers)
	}
	return binary.BigEndian.AppendUint32(frame, uint32(payloadSize))
}

func BeginTx(id uint32, route string) []byte {
	frame := []byte{OpBeginTx}
	frame = binary.BigEndian.AppendUint32(frame, id)
	return AppendString(frame, route)
}

func Correlated(op byte, id uint32) []byte {
	frame := []byte{op}
	return binary.BigEndian.AppendUint32(frame, id)
}

func Subscribe(op byte, id uint32, route string, autoSettle bool) []byte {
	frame := []byte{op}
	frame = binary.BigEndian.AppendUint32(frame, id)
	if autoSettle {
		frame = append(frame, 1)
	} else {
		frame = append(frame, 0)
	}
	return AppendString(frame, route)
}

func Fetch(op byte, id uint32, route string, autoSettle bool, maximum uint32) []byte {
	frame := Subscribe(op, id, route, autoSettle)
	return binary.BigEndian.AppendUint32(frame, maximum)
}

func Unsubscribe(id uint32, subscriptionID byte) []byte {
	frame := []byte{OpUnsubscribe}
	frame = binary.BigEndian.AppendUint32(frame, id)
	return append(frame, subscriptionID)
}

func Settle(op byte, id uint32, subscriptionID byte, messageIDs [][]byte) []byte {
	frame := []byte{op}
	frame = binary.BigEndian.AppendUint32(frame, id)
	frame = append(frame, subscriptionID)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(messageIDs)))
	for _, messageID := range messageIDs {
		frame = AppendBytes(frame, messageID)
	}
	return frame
}

type Reader struct{ r *bufio.Reader }

func NewReader(r io.Reader) *Reader   { return &Reader{r: bufio.NewReaderSize(r, 64*1024)} }
func (r *Reader) Byte() (byte, error) { return r.r.ReadByte() }

func (r *Reader) Uint16() (uint16, error) {
	value, err := r.r.Peek(2)
	if err != nil {
		return 0, err
	}
	result := binary.BigEndian.Uint16(value)
	_, err = r.r.Discard(2)
	return result, err
}

func (r *Reader) Uint32() (uint32, error) {
	value, err := r.r.Peek(4)
	if err != nil {
		return 0, err
	}
	result := binary.BigEndian.Uint32(value)
	_, err = r.r.Discard(4)
	return result, err
}

func (r *Reader) Bytes() ([]byte, error) {
	size, err := r.Uint32()
	if err != nil {
		return nil, err
	}
	value := make([]byte, int(size))
	_, err = io.ReadFull(r.r, value)
	return value, err
}

func (r *Reader) String() (string, error) {
	value, err := r.Bytes()
	return string(value), err
}

// BytesView returns a zero-copy view valid until the next read from Reader.
func (r *Reader) BytesView() ([]byte, error) {
	size, err := r.Uint32()
	if err != nil {
		return nil, err
	}
	if uint64(size) > uint64(r.r.Size()) {
		return nil, fmt.Errorf("zero-copy field size %d exceeds reader buffer", size)
	}
	value, err := r.r.Peek(int(size))
	if err != nil {
		return nil, err
	}
	if _, err := r.r.Discard(int(size)); err != nil {
		return nil, err
	}
	return value, nil
}

func (r *Reader) Status() (*fujin.OperationError, error) {
	status, err := r.Byte()
	if err != nil {
		return nil, err
	}
	if status == 0 {
		return nil, nil
	}
	outcome, err := r.Byte()
	if err != nil {
		return nil, err
	}
	reason, err := r.String()
	if err != nil {
		return nil, err
	}
	message, err := r.String()
	if err != nil {
		return nil, err
	}
	count, err := r.Uint16()
	if err != nil {
		return nil, err
	}
	details := make(map[string]string, int(count))
	for range count {
		key, err := r.String()
		if err != nil {
			return nil, err
		}
		value, err := r.String()
		if err != nil {
			return nil, err
		}
		details[key] = value
	}
	return &fujin.OperationError{Code: fujin.StatusCode(status), Outcome: fujin.OperationOutcome(outcome), Reason: reason, Message: message, Details: details}, nil
}

func (r *Reader) Headers() ([]fujin.Header, error) {
	count, err := r.Uint16()
	if err != nil {
		return nil, err
	}
	if count%2 != 0 {
		return nil, fmt.Errorf("invalid header field count %d", count)
	}
	headers := make([]fujin.Header, 0, count/2)
	for range count / 2 {
		key, err := r.Bytes()
		if err != nil {
			return nil, err
		}
		value, err := r.Bytes()
		if err != nil {
			return nil, err
		}
		headers = append(headers, fujin.Header{Key: key, Value: value})
	}
	return headers, nil
}

func (r *Reader) HelloResponse() (HelloInfo, error) {
	op, err := r.Byte()
	if err != nil {
		return HelloInfo{}, err
	}
	if op != RespHello {
		return HelloInfo{}, fmt.Errorf("unexpected HELLO response opcode %d", op)
	}
	operationErr, err := r.Status()
	if err != nil {
		return HelloInfo{}, err
	}
	if operationErr != nil {
		return HelloInfo{}, operationErr
	}
	format, err := r.Byte()
	if err != nil {
		return HelloInfo{}, err
	}
	if format != HelloFormat {
		return HelloInfo{}, fmt.Errorf("unsupported HELLO response format %d", format)
	}
	version, err := r.Byte()
	if err != nil {
		return HelloInfo{}, err
	}
	if WireVersion(version) != Version {
		return HelloInfo{}, fmt.Errorf("server selected unsupported protocol version %d", version)
	}
	serverBuild, err := r.BytesView()
	if err != nil {
		return HelloInfo{}, err
	}
	return HelloInfo{ProtocolVersion: WireVersion(version), ServerBuildVersion: serverBuild}, nil
}

func (r *Reader) BindResponse() (map[string]fujin.RouteCapabilities, error) {
	op, err := r.Byte()
	if err != nil {
		return nil, err
	}
	if op != RespBind {
		return nil, fmt.Errorf("unexpected BIND response opcode %d", op)
	}
	operationErr, err := r.Status()
	if err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	count, err := r.Uint32()
	if err != nil {
		return nil, err
	}
	routes := make(map[string]fujin.RouteCapabilities, int(count))
	for range count {
		route, err := r.String()
		if err != nil {
			return nil, err
		}
		profile := make([]byte, 4)
		if _, err := io.ReadFull(r.r, profile); err != nil {
			return nil, err
		}
		routes[route] = fujin.RouteCapabilities{
			Produce: profile[0]&1 != 0, Headers: profile[0]&2 != 0,
			Transactions: profile[0]&4 != 0, Subscribe: profile[0]&8 != 0,
			Fetch: profile[0]&16 != 0, ManualSettlement: profile[0]&32 != 0,
			ProduceGuarantee: fujin.ProduceGuarantee(profile[1]),
			AckGranularity:   fujin.AckGranularity(profile[2]), NackEffect: fujin.NackEffect(profile[3]),
		}
	}
	return routes, nil
}
