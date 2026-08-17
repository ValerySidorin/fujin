package test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
)

// protoReader reads and parses Fujin binary protocol responses from a buffered reader.
type protoReader struct {
	r *bufio.Reader
}

func newProtoReader(reader io.Reader) *protoReader {
	return &protoReader{r: bufio.NewReaderSize(reader, 64*1024)}
}

// readExact reads exactly n bytes from the reader.
func (p *protoReader) readExact(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(p.r, buf)
	return buf, err
}

func (p *protoReader) readByte() (byte, error) {
	return p.r.ReadByte()
}

func (p *protoReader) readUint32() (uint32, error) {
	buf, err := p.readExact(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (p *protoReader) readUint16() (uint16, error) {
	buf, err := p.readExact(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf), nil
}

func (p *protoReader) readCID() (uint32, error) {
	return p.readUint32()
}

func (p *protoReader) readString() (string, error) {
	valueLen, err := p.readUint32()
	if err != nil {
		return "", err
	}
	value, err := p.readExact(int(valueLen))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (p *protoReader) readStatus() (*v1.OperationError, error) {
	status, err := p.readByte()
	if err != nil {
		return nil, err
	}
	if v1.StatusCode(status) == v1.STATUS_OK {
		return nil, nil
	}
	outcome, err := p.readByte()
	if err != nil {
		return nil, err
	}
	reason, err := p.readString()
	if err != nil {
		return nil, err
	}
	message, err := p.readString()
	if err != nil {
		return nil, err
	}
	detailCount, err := p.readUint16()
	if err != nil {
		return nil, err
	}
	details := make(map[string]string, int(detailCount))
	for range detailCount {
		key, err := p.readString()
		if err != nil {
			return nil, err
		}
		value, err := p.readString()
		if err != nil {
			return nil, err
		}
		details[key] = value
	}
	return &v1.OperationError{
		Code: v1.StatusCode(status), Outcome: v1.OperationOutcome(outcome),
		Reason: reason, Message: message, Details: details,
	}, nil
}

type nativeRouteCapabilities struct {
	Produce          bool
	Headers          bool
	Transactions     bool
	Subscribe        bool
	Fetch            bool
	ManualSettlement bool
	ProduceGuarantee v1.ProduceGuarantee
	AckGranularity   v1.AckGranularity
	NackEffect       v1.NackEffect
}

// readBindResp reads and discards the capability snapshot from a BIND response.
func (p *protoReader) readBindResp() error {
	_, err := p.readBindCapabilities()
	return err
}

// readBindCapabilities reads: [16, 0, routeCount(u32), route entries].
// Each route entry is [route string, capability flags, produce guarantee, ACK granularity, NACK effect].
func (p *protoReader) readBindCapabilities() (map[string]nativeRouteCapabilities, error) {
	code, err := p.readByte()
	if err != nil {
		return nil, err
	}
	if code != byte(v1.RESP_CODE_BIND) {
		return nil, fmt.Errorf("expected BIND resp code %d, got %d", v1.RESP_CODE_BIND, code)
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return nil, fmt.Errorf("bind status: %w", err)
	}
	if operationErr != nil {
		return nil, fmt.Errorf("bind failed: %w", operationErr)
	}

	routeCount, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	routes := make(map[string]nativeRouteCapabilities, int(routeCount))
	for range routeCount {
		routeLen, err := p.readUint32()
		if err != nil {
			return nil, err
		}
		route, err := p.readExact(int(routeLen))
		if err != nil {
			return nil, err
		}
		profile, err := p.readExact(4)
		if err != nil {
			return nil, err
		}
		flags := profile[0]
		routes[string(route)] = nativeRouteCapabilities{
			Produce:          flags&v1.ROUTE_CAP_PRODUCE != 0,
			Headers:          flags&v1.ROUTE_CAP_HEADERS != 0,
			Transactions:     flags&v1.ROUTE_CAP_TRANSACTIONS != 0,
			Subscribe:        flags&v1.ROUTE_CAP_SUBSCRIBE != 0,
			Fetch:            flags&v1.ROUTE_CAP_FETCH != 0,
			ManualSettlement: flags&v1.ROUTE_CAP_MANUAL_SETTLEMENT != 0,
			ProduceGuarantee: v1.ProduceGuarantee(profile[1]),
			AckGranularity:   v1.AckGranularity(profile[2]),
			NackEffect:       v1.NackEffect(profile[3]),
		}
	}
	return routes, nil
}

// readProduceResp reads a PRODUCE response: [3, cID(4), status, (error envelope)?]
func (p *protoReader) readProduceResp() (cID uint32, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, err
	}
	if code != byte(v1.RESP_CODE_PRODUCE) {
		return 0, fmt.Errorf("expected PRODUCE resp code %d, got %d", v1.RESP_CODE_PRODUCE, code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, err
	}
	if operationErr != nil {
		return cID, fmt.Errorf("produce error: %w", operationErr)
	}
	return cID, nil
}

// readHProduceResp reads an HPRODUCE response: [4, cID(4), status, (error envelope)?]
func (p *protoReader) readHProduceResp() (cID uint32, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, err
	}
	if code != byte(v1.RESP_CODE_HPRODUCE) {
		return 0, fmt.Errorf("expected HPRODUCE resp code %d, got %d", v1.RESP_CODE_HPRODUCE, code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, err
	}
	if operationErr != nil {
		return cID, fmt.Errorf("hproduce error: %w", operationErr)
	}
	return cID, nil
}

// readSubscribeResp reads SUBSCRIBE/HSUBSCRIBE response: [code, cID(4), status, subID?]
func (p *protoReader) readSubscribeResp() (cID uint32, subID byte, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, 0, err
	}
	if code != byte(v1.RESP_CODE_SUBSCRIBE) && code != byte(v1.RESP_CODE_HSUBSCRIBE) {
		return 0, 0, fmt.Errorf("expected SUBSCRIBE/HSUBSCRIBE resp code, got %d", code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, 0, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, 0, err
	}
	if operationErr != nil {
		return cID, 0, fmt.Errorf("subscribe error: %w", operationErr)
	}
	subID, err = p.readByte()
	return cID, subID, err
}

// fetchedMsg represents a single message from a FETCH response.
type fetchedMsg struct {
	MsgID   []byte   // non-nil only if autoCommit=false
	Headers [][]byte // non-nil only for HFETCH
	Payload []byte
}

// readFetchResp reads FETCH response: [10, cID(4), status, subID, count(4), msgs...]
// Each msg (autoCommit): [msgLen(4), msg]
// Each msg (!autoCommit): [msgIDLen(4), msgID, msgLen(4), msg]
func (p *protoReader) readFetchResp(autoCommit bool) (cID uint32, subID byte, msgs []fetchedMsg, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, 0, nil, err
	}
	if code != byte(v1.RESP_CODE_FETCH) && code != byte(v1.RESP_CODE_HFETCH) {
		return 0, 0, nil, fmt.Errorf("expected FETCH/HFETCH resp code, got %d", code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, 0, nil, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, 0, nil, err
	}
	if operationErr != nil {
		return cID, 0, nil, fmt.Errorf("fetch error: %w", operationErr)
	}
	subID, err = p.readByte()
	if err != nil {
		return cID, 0, nil, err
	}
	count, err := p.readUint32()
	if err != nil {
		return cID, subID, nil, err
	}
	msgs = make([]fetchedMsg, 0, count)
	for i := uint32(0); i < count; i++ {
		var msg fetchedMsg
		if !autoCommit {
			msgIDLen, err := p.readUint32()
			if err != nil {
				return cID, subID, msgs, err
			}
			msg.MsgID, err = p.readExact(int(msgIDLen))
			if err != nil {
				return cID, subID, msgs, err
			}
		}
		msgLen, err := p.readUint32()
		if err != nil {
			return cID, subID, msgs, err
		}
		msg.Payload, err = p.readExact(int(msgLen))
		if err != nil {
			return cID, subID, msgs, err
		}
		msgs = append(msgs, msg)
	}
	return cID, subID, msgs, nil
}

// readHFetchResp reads HFETCH response: [11, cID(4), status, subID, count(4), msgs...]
// Each msg (autoCommit): [headersCount(2), headers..., msgLen(4), msg]
// Each msg (!autoCommit): [headersCount(2), headers..., msgIDLen(4), msgID, msgLen(4), msg]
func (p *protoReader) readHFetchResp(autoCommit bool) (cID uint32, subID byte, msgs []fetchedMsg, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, 0, nil, err
	}
	if code != byte(v1.RESP_CODE_HFETCH) {
		return 0, 0, nil, fmt.Errorf("expected HFETCH resp code %d, got %d", v1.RESP_CODE_HFETCH, code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, 0, nil, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, 0, nil, err
	}
	if operationErr != nil {
		return cID, 0, nil, fmt.Errorf("hfetch error: %w", operationErr)
	}
	subID, err = p.readByte()
	if err != nil {
		return cID, 0, nil, err
	}
	count, err := p.readUint32()
	if err != nil {
		return cID, subID, nil, err
	}
	msgs = make([]fetchedMsg, 0, count)
	for i := uint32(0); i < count; i++ {
		var msg fetchedMsg
		headersCount, err := p.readUint16()
		if err != nil {
			return cID, subID, msgs, err
		}
		msg.Headers = make([][]byte, headersCount)
		for j := uint16(0); j < headersCount; j++ {
			hdrLen, err := p.readUint32()
			if err != nil {
				return cID, subID, msgs, err
			}
			msg.Headers[j], err = p.readExact(int(hdrLen))
			if err != nil {
				return cID, subID, msgs, err
			}
		}
		if !autoCommit {
			msgIDLen, err := p.readUint32()
			if err != nil {
				return cID, subID, msgs, err
			}
			msg.MsgID, err = p.readExact(int(msgIDLen))
			if err != nil {
				return cID, subID, msgs, err
			}
		}
		msgLen, err := p.readUint32()
		if err != nil {
			return cID, subID, msgs, err
		}
		msg.Payload, err = p.readExact(int(msgLen))
		if err != nil {
			return cID, subID, msgs, err
		}
		msgs = append(msgs, msg)
	}
	return cID, subID, msgs, nil
}

// readMsg reads MSG response (autoCommit=true): [8, subID, msgLen(4), msg]
func (p *protoReader) readMsg() (subID byte, payload []byte, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, nil, err
	}
	if code != byte(v1.RESP_CODE_MSG) {
		return 0, nil, fmt.Errorf("expected MSG resp code %d, got %d", v1.RESP_CODE_MSG, code)
	}
	subID, err = p.readByte()
	if err != nil {
		return 0, nil, err
	}
	msgLen, err := p.readUint32()
	if err != nil {
		return subID, nil, err
	}
	payload, err = p.readExact(int(msgLen))
	if err != nil {
		return subID, nil, err
	}
	return subID, payload, nil
}

// readMsgWithID reads MSG response (autoCommit=false): [8, msgIDLen(4), msgID, msgLen(4), msg]
func (p *protoReader) readMsgWithID() (msgID []byte, payload []byte, err error) {
	code, err := p.readByte()
	if err != nil {
		return nil, nil, err
	}
	if code != byte(v1.RESP_CODE_MSG) {
		return nil, nil, fmt.Errorf("expected MSG resp code %d, got %d", v1.RESP_CODE_MSG, code)
	}
	msgIDLen, err := p.readUint32()
	if err != nil {
		return nil, nil, err
	}
	msgID, err = p.readExact(int(msgIDLen))
	if err != nil {
		return nil, nil, err
	}
	msgLen, err := p.readUint32()
	if err != nil {
		return msgID, nil, err
	}
	payload, err = p.readExact(int(msgLen))
	if err != nil {
		return msgID, nil, err
	}
	return msgID, payload, nil
}

// readHMsg reads HMSG response (autoCommit=true): [9, subID, headersCount(2), headers..., msgLen(4), msg]
func (p *protoReader) readHMsg() (subID byte, headers [][]byte, payload []byte, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, nil, nil, err
	}
	if code != byte(v1.RESP_CODE_HMSG) {
		return 0, nil, nil, fmt.Errorf("expected HMSG resp code %d, got %d", v1.RESP_CODE_HMSG, code)
	}
	subID, err = p.readByte()
	if err != nil {
		return 0, nil, nil, err
	}
	headersCount, err := p.readUint16()
	if err != nil {
		return subID, nil, nil, err
	}
	headers = make([][]byte, headersCount)
	for i := uint16(0); i < headersCount; i++ {
		hdrLen, err := p.readUint32()
		if err != nil {
			return subID, headers, nil, err
		}
		headers[i], err = p.readExact(int(hdrLen))
		if err != nil {
			return subID, headers, nil, err
		}
	}
	msgLen, err := p.readUint32()
	if err != nil {
		return subID, headers, nil, err
	}
	payload, err = p.readExact(int(msgLen))
	if err != nil {
		return subID, headers, nil, err
	}
	return subID, headers, payload, nil
}

// ackResult represents a single ACK/NACK result.
type ackResult struct {
	MsgID []byte
	Err   error // nil if success
}

// readAckResp reads ACK response: [12, cID(4), status, count(4), results...]
// Each result: [msgIDLen(4), msgID, status, (error envelope)?]
func (p *protoReader) readAckResp() (cID uint32, results []ackResult, err error) {
	return p.readSettlementResp(v1.RESP_CODE_ACK, "ack")
}

// readNackResp reads NACK response (same format as ACK but code 13).
func (p *protoReader) readNackResp() (cID uint32, results []ackResult, err error) {
	return p.readSettlementResp(v1.RESP_CODE_NACK, "nack")
}

func (p *protoReader) readSettlementResp(expectedCode v1.RespCode, operation string) (cID uint32, results []ackResult, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, nil, err
	}
	if code != byte(expectedCode) {
		return 0, nil, fmt.Errorf("expected %s resp code %d, got %d", operation, expectedCode, code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, nil, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, nil, err
	}
	if operationErr != nil {
		return cID, nil, fmt.Errorf("%s error: %w", operation, operationErr)
	}
	count, err := p.readUint32()
	if err != nil {
		return cID, nil, err
	}
	results = make([]ackResult, count)
	for i := uint32(0); i < count; i++ {
		msgIDLen, err := p.readUint32()
		if err != nil {
			return cID, results, err
		}
		results[i].MsgID, err = p.readExact(int(msgIDLen))
		if err != nil {
			return cID, results, err
		}
		operationErr, err := p.readStatus()
		if err != nil {
			return cID, results, err
		}
		if operationErr != nil {
			results[i].Err = operationErr
		}
	}
	return cID, results, nil
}

// readTxResp reads a transaction response: [code, cID(4), status, (error envelope)?]
func (p *protoReader) readTxResp(expectedCode v1.RespCode) (cID uint32, err error) {
	code, err := p.readByte()
	if err != nil {
		return 0, err
	}
	if code != byte(expectedCode) {
		return 0, fmt.Errorf("expected tx resp code %d, got %d", expectedCode, code)
	}
	cID, err = p.readCID()
	if err != nil {
		return 0, err
	}
	operationErr, err := p.readStatus()
	if err != nil {
		return cID, err
	}
	if operationErr != nil {
		return cID, fmt.Errorf("tx error: %w", operationErr)
	}
	return cID, nil
}

// --- Command builders ---

func buildProduceCmd(cID uint32, route, payload string) []byte {
	cmd := []byte{byte(v1.OP_CODE_PRODUCE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = appendFujinString(cmd, route)
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(payload)))
	cmd = append(cmd, payload...)
	return cmd
}

func buildHProduceCmd(cID uint32, route string, headers [][2]string, payload string) []byte {
	cmd := []byte{byte(v1.OP_CODE_HPRODUCE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = appendFujinString(cmd, route)
	cmd = binary.BigEndian.AppendUint16(cmd, uint16(len(headers)*2))
	for _, header := range headers {
		cmd = appendFujinString(cmd, header[0])
		cmd = appendFujinString(cmd, header[1])
	}
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(payload)))
	cmd = append(cmd, payload...)
	return cmd
}

func buildSubscribeCmd2(cID uint32, autoCommit bool, route string) []byte {
	cmd := []byte{byte(v1.OP_CODE_SUBSCRIBE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	if autoCommit {
		cmd = append(cmd, 1)
	} else {
		cmd = append(cmd, 0)
	}
	cmd = appendFujinString(cmd, route)
	return cmd
}

func buildHSubscribeCmd(cID uint32, autoCommit bool, route string) []byte {
	cmd := []byte{byte(v1.OP_CODE_HSUBSCRIBE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	if autoCommit {
		cmd = append(cmd, 1)
	} else {
		cmd = append(cmd, 0)
	}
	cmd = appendFujinString(cmd, route)
	return cmd
}

func buildFetchCmd2(cID uint32, autoCommit bool, route string, n uint32) []byte {
	cmd := []byte{byte(v1.OP_CODE_FETCH)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	if autoCommit {
		cmd = append(cmd, 1)
	} else {
		cmd = append(cmd, 0)
	}
	cmd = appendFujinString(cmd, route)
	cmd = binary.BigEndian.AppendUint32(cmd, n)
	return cmd
}

func buildHFetchCmd(cID uint32, autoCommit bool, route string, n uint32) []byte {
	cmd := []byte{byte(v1.OP_CODE_HFETCH)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	if autoCommit {
		cmd = append(cmd, 1)
	} else {
		cmd = append(cmd, 0)
	}
	cmd = appendFujinString(cmd, route)
	cmd = binary.BigEndian.AppendUint32(cmd, n)
	return cmd
}

func buildAckCmd(cID uint32, subID byte, msgIDs [][]byte) []byte {
	cmd := []byte{byte(v1.OP_CODE_ACK)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = append(cmd, subID)
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(msgIDs)))
	for _, msgID := range msgIDs {
		cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(msgID)))
		cmd = append(cmd, msgID...)
	}
	return cmd
}

func buildNackCmd(cID uint32, subID byte, msgIDs [][]byte) []byte {
	cmd := []byte{byte(v1.OP_CODE_NACK)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = append(cmd, subID)
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(msgIDs)))
	for _, msgID := range msgIDs {
		cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(msgID)))
		cmd = append(cmd, msgID...)
	}
	return cmd
}

func buildTxBeginCmd(cID uint32, route string) []byte {
	cmd := []byte{byte(v1.OP_CODE_TX_BEGIN)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	return appendFujinString(cmd, route)
}

func buildTxProduceCmd(cID uint32, payload string) []byte {
	cmd := []byte{byte(v1.OP_CODE_TX_PRODUCE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(payload)))
	cmd = append(cmd, payload...)
	return cmd
}

func buildTxHProduceCmd(cID uint32, headers [][2]string, payload string) []byte {
	cmd := []byte{byte(v1.OP_CODE_TX_HPRODUCE)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	cmd = binary.BigEndian.AppendUint16(cmd, uint16(len(headers)*2))
	for _, header := range headers {
		cmd = appendFujinString(cmd, header[0])
		cmd = appendFujinString(cmd, header[1])
	}
	cmd = binary.BigEndian.AppendUint32(cmd, uint32(len(payload)))
	cmd = append(cmd, payload...)
	return cmd
}

func buildTxCommitCmd(cID uint32) []byte {
	cmd := []byte{byte(v1.OP_CODE_TX_COMMIT)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	return cmd
}

func buildTxRollbackCmd(cID uint32) []byte {
	cmd := []byte{byte(v1.OP_CODE_TX_ROLLBACK)}
	cmd = binary.BigEndian.AppendUint32(cmd, cID)
	return cmd
}

func buildDisconnectCmd() []byte {
	return []byte{byte(v1.OP_CODE_DISCONNECT)}
}

// --- E2E test infrastructure ---

// e2eCapabilities describes what operations a connector supports.
type e2eCapabilities struct {
	produce    bool
	hproduce   bool
	fetch      bool
	hfetch     bool
	subscribe  bool
	hsubscribe bool
	ack        bool
	nack       bool
	tx         bool

	// ackConnector overrides the connector name for ack/nack tests.
	// Useful for Kafka where a separate consumer group avoids rebalancing conflicts.
	ackConnector  string
	nackConnector string
}

// e2eConn wraps a TCP connection with protocol reader and write helper.
type e2eConn struct {
	conn net.Conn
	pr   *protoReader
	t    *testing.T
}

func newE2EConn(t *testing.T, addr string) *e2eConn {
	t.Helper()
	conn := createTCPClientConn(addr)
	return &e2eConn{
		conn: conn,
		pr:   newProtoReader(conn),
		t:    t,
	}
}

func (c *e2eConn) close() {
	c.conn.Close()
}

func (c *e2eConn) write(data []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(data); err != nil {
		c.t.Fatalf("write failed: %v", err)
	}
}

func (c *e2eConn) setDeadline(d time.Duration) {
	c.conn.SetDeadline(time.Now().Add(d))
}

func (c *e2eConn) bindAndRead(connector string) {
	c.t.Helper()
	c.write(bindCmd(connector, nil, nil))
	if err := c.pr.readBindResp(); err != nil {
		c.t.Fatalf("bind failed: %v", err)
	}
}
