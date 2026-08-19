package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/server"
)

const (
	e2eTimeout      = 30 * time.Second
	e2eFetchTimeout = 60 * time.Second
)

// --- Per-connector test functions ---

func TestE2E_KafkaFranz(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithKafka3BrokersTCP, "pub", "sub", e2eCapabilities{
		produce:       true,
		hproduce:      true,
		fetch:         true,
		hfetch:        true,
		subscribe:     true,
		hsubscribe:    true,
		ack:           true,
		nack:          true,
		ackConnector:  "ack_connector",
		nackConnector: "nack_connector",
		// tx requires TransactionalID in kafka config; tested separately if needed
	})
}

func TestE2E_NatsCore(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithNatsTCP, "pub", "sub", e2eCapabilities{
		produce:    true,
		hproduce:   true,
		subscribe:  true,
		hsubscribe: true,
	})
}

func TestE2E_NatsJetstream(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithNatsJetstreamTCP, "pub", "sub", e2eCapabilities{
		produce:       true,
		hproduce:      true,
		fetch:         true,
		hfetch:        true,
		subscribe:     true,
		hsubscribe:    true,
		ack:           true,
		nack:          true,
		ackConnector:  "ack_connector",
		nackConnector: "nack_connector",
	})
}

func TestE2E_RabbitMQ(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithAMQP091TCP, "pub", "sub", e2eCapabilities{
		produce:    true,
		hproduce:   true,
		subscribe:  true,
		hsubscribe: true,
		ack:        true,
		nack:       true,
		tx:         true,
	})
}

func TestE2E_AzureAMQP1(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithAMQP10TCP, "pub", "sub", e2eCapabilities{
		produce:    true,
		hproduce:   true,
		subscribe:  true,
		hsubscribe: true,
		ack:        true,
		nack:       true,
	})
}

func TestE2E_RedisPubSub(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithRedisPubSubTCP, "pub", "sub", e2eCapabilities{
		produce:   true,
		hproduce:  true,
		subscribe: true,
	})
}

func TestE2E_RedisStreams(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithRedisStreamsTCP, "pub", "sub", e2eCapabilities{
		produce:   true,
		subscribe: true,
		fetch:     true,
		ack:       true,
	})
}

func TestE2E_MQTT(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithMQTTTCP, "pub", "sub", e2eCapabilities{
		produce:    true,
		hproduce:   true,
		subscribe:  true,
		hsubscribe: true,
	})
}

func TestE2E_NSQ(t *testing.T) {
	runE2ESuite(t, RunDefaultServerWithNSQTCP, "pub", "sub", e2eCapabilities{
		produce:   true,
		subscribe: true,
	})
}

// --- Main test suite ---

func runE2ESuite(
	t *testing.T,
	startServer func(ctx context.Context) *server.Server,
	pubClient, subClient string,
	caps e2eCapabilities,
) {
	if os.Getenv("FUJIN_E2E") != "1" {
		t.Skip("broker-backed E2E disabled; set FUJIN_E2E=1")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := startServer(ctx)
	defer func() {
		cancel()
		<-s.Done()
	}()

	// Give broker a moment to stabilize
	time.Sleep(500 * time.Millisecond)

	t.Run("Produce", func(t *testing.T) {
		if !caps.produce {
			t.Skip("produce not supported")
		}
		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eTimeout)
		c.bindAndRead("connector")

		cmd := buildProduceCmd(1, pubClient, "hello-produce")
		c.write(cmd)

		cID, err := c.pr.readProduceResp()
		if err != nil {
			t.Fatalf("produce response error: %v", err)
		}
		if cID != 1 {
			t.Fatalf("expected cID=1, got %d", cID)
		}
	})

	t.Run("HProduce", func(t *testing.T) {
		if !caps.hproduce {
			t.Skip("hproduce not supported")
		}
		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eTimeout)
		c.bindAndRead("connector")

		headers := [][2]string{
			{"key1", "val1"},
			{"key2", "val2"},
		}
		cmd := buildHProduceCmd(2, pubClient, headers, "hello-hproduce")
		c.write(cmd)

		cID, err := c.pr.readHProduceResp()
		if err != nil {
			t.Fatalf("hproduce response error: %v", err)
		}
		if cID != 2 {
			t.Fatalf("expected cID=2, got %d", cID)
		}
	})

	t.Run("Produce_Subscribe", func(t *testing.T) {
		if !caps.produce || !caps.subscribe {
			t.Skip("produce or subscribe not supported")
		}

		payload := fmt.Sprintf("e2e-subscribe-%d", time.Now().UnixNano())

		cSub := newE2EConn(t, PERF_TCP_ADDR)
		defer cSub.close()
		cSub.setDeadline(e2eTimeout)
		cSub.bindAndRead("connector")
		cSub.write(buildSubscribeCmd2(10, true, subClient))
		_, _, err := cSub.pr.readSubscribeResp()
		if err != nil {
			t.Fatalf("subscribe response error: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		cPub := newE2EConn(t, PERF_TCP_ADDR)
		defer cPub.close()
		cPub.setDeadline(e2eTimeout)
		cPub.bindAndRead("connector")
		cPub.write(buildProduceCmd(11, pubClient, payload))
		_, err = cPub.pr.readProduceResp()
		if err != nil {
			t.Fatalf("produce error: %v", err)
		}

		// Read MSGs until we find ours (may receive old messages first)
		for i := 0; i < 100; i++ {
			_, gotPayload, err := cSub.pr.readMsg()
			if err != nil {
				t.Fatalf("read msg error: %v", err)
			}
			if strings.Contains(string(gotPayload), payload) {
				return
			}
		}
		t.Fatalf("payload %q not found after 100 messages", payload)
	})

	t.Run("Produce_HSubscribe", func(t *testing.T) {
		if !caps.produce || !caps.hsubscribe {
			t.Skip("hsubscribe not supported")
		}

		payload := fmt.Sprintf("e2e-hsubscribe-%d", time.Now().UnixNano())

		cSub := newE2EConn(t, PERF_TCP_ADDR)
		defer cSub.close()
		cSub.setDeadline(e2eTimeout)
		cSub.bindAndRead("connector")
		cSub.write(buildHSubscribeCmd(20, true, subClient))
		_, _, err := cSub.pr.readSubscribeResp()
		if err != nil {
			t.Fatalf("hsubscribe response error: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		cPub := newE2EConn(t, PERF_TCP_ADDR)
		defer cPub.close()
		cPub.setDeadline(e2eTimeout)
		cPub.bindAndRead("connector")
		headers := [][2]string{{"hdr-key", "hdr-val"}}
		cPub.write(buildHProduceCmd(21, pubClient, headers, payload))
		_, err = cPub.pr.readHProduceResp()
		if err != nil {
			t.Fatalf("hproduce error: %v", err)
		}

		for i := 0; i < 100; i++ {
			_, _, gotPayload, err := cSub.pr.readHMsg()
			if err != nil {
				t.Fatalf("read hmsg error: %v", err)
			}
			if strings.Contains(string(gotPayload), payload) {
				return
			}
		}
		t.Fatalf("payload %q not found after 100 messages", payload)
	})

	// Small pause to let previous consumers (subscribe) release before fetch tests.
	time.Sleep(1 * time.Second)

	// Fetch and HFetch use separate sessions. A cached Kafka FETCH reader stays
	// in its consumer group until the session closes; keeping it alive while
	// creating the HFETCH reader can leave the single topic partition assigned
	// to the idle reader and make HFETCH wait indefinitely.
	t.Run("Fetch_Flow", func(t *testing.T) {
		if !caps.produce || !caps.fetch {
			t.Skip("fetch not supported")
		}

		var cIDCounter uint32 = 100

		nextCID := func() uint32 {
			cIDCounter++
			return cIDCounter
		}

		// --- Subtest: Fetch ---
		t.Run("Produce_Fetch", func(t *testing.T) {
			c := newE2EConn(t, PERF_TCP_ADDR)
			defer c.close()
			c.setDeadline(e2eTimeout)
			c.bindAndRead("connector")

			payload := fmt.Sprintf("e2e-fetch-%d", time.Now().UnixNano())

			cPub := newE2EConn(t, PERF_TCP_ADDR)
			defer cPub.close()
			cPub.setDeadline(e2eTimeout)
			cPub.bindAndRead("connector")
			cPub.write(buildProduceCmd(nextCID(), pubClient, payload))
			_, err := cPub.pr.readProduceResp()
			if err != nil {
				t.Fatalf("produce error: %v", err)
			}
			time.Sleep(1 * time.Second)

			for attempt := 0; attempt < 10; attempt++ {
				c.write(buildFetchCmd2(nextCID(), true, subClient, 100))
				_, _, msgs, err := c.pr.readFetchResp(true)
				if err != nil {
					t.Fatalf("fetch error: %v", err)
				}
				for _, m := range msgs {
					if strings.Contains(string(m.Payload), payload) {
						return
					}
				}
				time.Sleep(500 * time.Millisecond)
			}
			t.Fatalf("payload not found in fetched messages")
		})

		// --- Subtest: HFetch ---
		t.Run("Produce_HFetch", func(t *testing.T) {
			if !caps.hfetch {
				t.Skip("hfetch not supported")
			}
			c := newE2EConn(t, PERF_TCP_ADDR)
			defer c.close()
			c.setDeadline(e2eTimeout)
			c.bindAndRead("connector")

			payload := fmt.Sprintf("e2e-hfetch-%d", time.Now().UnixNano())

			cPub := newE2EConn(t, PERF_TCP_ADDR)
			defer cPub.close()
			cPub.setDeadline(e2eTimeout)
			cPub.bindAndRead("connector")
			headers := [][2]string{{"hf-key", "hf-val"}}
			cPub.write(buildHProduceCmd(nextCID(), pubClient, headers, payload))
			_, err := cPub.pr.readHProduceResp()
			if err != nil {
				t.Fatalf("hproduce error: %v", err)
			}
			time.Sleep(1 * time.Second)

			for attempt := 0; attempt < 10; attempt++ {
				c.write(buildHFetchCmd(nextCID(), true, subClient, 100))
				_, _, msgs, err := c.pr.readHFetchResp(true)
				if err != nil {
					t.Fatalf("hfetch error: %v", err)
				}
				for _, m := range msgs {
					if strings.Contains(string(m.Payload), payload) {
						return
					}
				}
				time.Sleep(500 * time.Millisecond)
			}
			t.Fatalf("payload not found in hfetched messages")
		})
	})

	// Ack/Nack need autoCommit=false fetch. When a separate ackConnector is
	// configured (e.g. Kafka with its own consumer group), use it to avoid
	// consumer group rebalancing conflicts with subscribe/fetch tests.
	ackConn := "connector"
	if caps.ackConnector != "" {
		ackConn = caps.ackConnector
	}
	nackConn := "connector"
	if caps.nackConnector != "" {
		nackConn = caps.nackConnector
	}

	t.Run("Produce_Fetch_Ack", func(t *testing.T) {
		if !caps.produce || !caps.fetch || !caps.ack {
			t.Skip("fetch+ack not supported")
		}

		payload := fmt.Sprintf("e2e-ack-%d", time.Now().UnixNano())

		// Produce via separate connection
		cPub := newE2EConn(t, PERF_TCP_ADDR)
		defer cPub.close()
		cPub.setDeadline(e2eTimeout)
		cPub.bindAndRead(ackConn)
		cPub.write(buildProduceCmd(201, pubClient, payload))
		_, err := cPub.pr.readProduceResp()
		if err != nil {
			t.Fatalf("produce error: %v", err)
		}
		time.Sleep(1 * time.Second)

		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eFetchTimeout)
		c.bindAndRead(ackConn)

		var fetchSubID byte
		var msgIDs [][]byte
		for attempt := 0; attempt < 20; attempt++ {
			c.write(buildFetchCmd2(202+uint32(attempt), false, subClient, 100))
			_, sid, msgs, err := c.pr.readFetchResp(false)
			if err != nil {
				t.Fatalf("fetch error: %v", err)
			}
			fetchSubID = sid
			for _, m := range msgs {
				if m.MsgID != nil {
					msgIDs = append(msgIDs, m.MsgID)
				}
			}
			if len(msgIDs) > 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if len(msgIDs) == 0 {
			t.Fatal("no msgIDs to ack after retries")
		}

		c.write(buildAckCmd(220, fetchSubID, msgIDs))
		_, results, err := c.pr.readAckResp()
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("ack result[%d] error: %v", i, r.Err)
			}
		}
	})

	t.Run("Produce_Fetch_Nack", func(t *testing.T) {
		if !caps.produce || !caps.fetch || !caps.nack {
			t.Skip("fetch+nack not supported")
		}

		payload := fmt.Sprintf("e2e-nack-%d", time.Now().UnixNano())

		cPub := newE2EConn(t, PERF_TCP_ADDR)
		defer cPub.close()
		cPub.setDeadline(e2eTimeout)
		cPub.bindAndRead(nackConn)
		cPub.write(buildProduceCmd(301, pubClient, payload))
		_, err := cPub.pr.readProduceResp()
		if err != nil {
			t.Fatalf("produce error: %v", err)
		}
		time.Sleep(1 * time.Second)

		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eFetchTimeout)
		c.bindAndRead(nackConn)

		var fetchSubID byte
		var msgIDs [][]byte
		for attempt := 0; attempt < 20; attempt++ {
			c.write(buildFetchCmd2(302+uint32(attempt), false, subClient, 100))
			_, sid, msgs, err := c.pr.readFetchResp(false)
			if err != nil {
				t.Fatalf("fetch error: %v", err)
			}
			fetchSubID = sid
			for _, m := range msgs {
				if m.MsgID != nil {
					msgIDs = append(msgIDs, m.MsgID)
				}
			}
			if len(msgIDs) > 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if len(msgIDs) == 0 {
			t.Fatal("no msgIDs to nack after retries")
		}

		c.write(buildNackCmd(320, fetchSubID, msgIDs))
		_, results, err := c.pr.readNackResp()
		if err != nil {
			t.Fatalf("nack error: %v", err)
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("nack result[%d] error: %v", i, r.Err)
			}
		}
	})

	t.Run("Tx_Produce_Commit", func(t *testing.T) {
		if !caps.tx || !caps.produce {
			t.Skip("tx not supported")
		}

		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eTimeout)
		c.bindAndRead("connector")

		c.write(buildTxBeginCmd(70, pubClient))
		cID, err := c.pr.readTxResp(v1.RESP_CODE_TX_BEGIN)
		if err != nil {
			t.Fatalf("tx_begin error: %v", err)
		}
		if cID != 70 {
			t.Fatalf("expected cID=70, got %d", cID)
		}

		c.write(buildTxProduceCmd(71, "tx-commit-payload"))
		_, err = c.pr.readTxResp(v1.RESP_CODE_TX_PRODUCE)
		if err != nil {
			t.Fatalf("tx_produce error: %v", err)
		}

		c.write(buildTxCommitCmd(72))
		_, err = c.pr.readTxResp(v1.RESP_CODE_TX_COMMIT)
		if err != nil {
			t.Fatalf("tx_commit error: %v", err)
		}
	})

	t.Run("Tx_Produce_Rollback", func(t *testing.T) {
		if !caps.tx || !caps.produce {
			t.Skip("tx not supported")
		}

		c := newE2EConn(t, PERF_TCP_ADDR)
		defer c.close()
		c.setDeadline(e2eTimeout)
		c.bindAndRead("connector")

		c.write(buildTxBeginCmd(80, pubClient))
		_, err := c.pr.readTxResp(v1.RESP_CODE_TX_BEGIN)
		if err != nil {
			t.Fatalf("tx_begin error: %v", err)
		}

		c.write(buildTxProduceCmd(81, "tx-rollback-payload"))
		_, err = c.pr.readTxResp(v1.RESP_CODE_TX_PRODUCE)
		if err != nil {
			t.Fatalf("tx_produce error: %v", err)
		}

		c.write(buildTxRollbackCmd(82))
		_, err = c.pr.readTxResp(v1.RESP_CODE_TX_ROLLBACK)
		if err != nil {
			t.Fatalf("tx_rollback error: %v", err)
		}
	})
}
