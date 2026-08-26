package fujin_test

import (
	"context"
	"testing"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/fujin"
	"github.com/fujin-io/fujin/sdk/go/client/internal/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativeClientSessionContract(t *testing.T) {
	server := startNativeTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := fujin.Dial(ctx, server.addr(), server.clientTLS, nil)
	require.NoError(t, err)
	defer conn.Close()

	stream, err := conn.Bind(ctx, "connector", client.WithMeta(map[string]string{"tenant": "test"}), client.WithConfigOverrides(map[string]string{"writer.pub.mode": "test"}))
	require.NoError(t, err)

	routes := stream.Routes()
	require.Len(t, routes, 3)
	assert.Equal(t, client.RouteCapabilities{Produce: true, Headers: true, ProduceGuarantee: client.ProduceGuaranteeLocalAccept}, routes["pub"])
	assert.Equal(t, client.RouteCapabilities{Produce: true, Headers: true, Transactions: true, ProduceGuarantee: client.ProduceGuaranteeLocalAccept}, routes["tx"])
	assert.Equal(t, client.RouteCapabilities{Headers: true, Subscribe: true, Fetch: true, ManualSettlement: true, AckGranularity: client.AckSingle, NackEffect: client.NackDrop}, routes["sub"])
	delete(routes, "pub")
	assert.Contains(t, stream.Routes(), "pub")

	require.NoError(t, stream.Produce(ctx, "pub", []byte("message")))
	require.NoError(t, stream.HProduce(ctx, "pub", []byte("message"), []client.Header{{Key: []byte("key"), Value: []byte("value")}}))

	err = stream.Produce(ctx, "missing", []byte("message"))
	var operationErr *client.OperationError
	require.ErrorAs(t, err, &operationErr)
	assert.Equal(t, client.StatusNotFound, operationErr.Code)
	assert.Equal(t, client.OutcomeNotApplied, operationErr.Outcome)
	assert.Equal(t, "ROUTE_NOT_FOUND", operationErr.Reason)

	require.NoError(t, stream.BeginTx(ctx, "tx"))
	require.ErrorIs(t, stream.Produce(ctx, "pub", []byte("not-transactional")), session.ErrTransactionActive)
	require.NoError(t, stream.TxProduce(ctx, []byte("transaction-message")))
	require.NoError(t, stream.TxHProduce(ctx, []byte("transaction-message"), []client.Header{{Key: []byte("key"), Value: []byte("value")}}))
	require.NoError(t, stream.CommitTx(ctx))

	require.NoError(t, stream.BeginTx(ctx, "tx"))
	require.NoError(t, stream.RollbackTx(ctx))
	require.ErrorIs(t, stream.CommitTx(ctx), session.ErrNoTransaction)

	fetch, err := stream.Fetch(ctx, "sub", false, 1)
	require.NoError(t, err)
	assert.Equal(t, uint32(9), fetch.SubscriptionID)
	require.Len(t, fetch.Messages, 1)
	assert.Equal(t, []byte("fetch-id"), fetch.Messages[0].MessageID)
	assert.Equal(t, []byte("fetched-message"), fetch.Messages[0].Payload)

	hfetch, err := stream.HFetch(ctx, "sub", false, 1)
	require.NoError(t, err)
	require.Len(t, hfetch.Messages, 1)
	assert.Equal(t, []client.Header{{Key: []byte("content-type"), Value: []byte("application/octet-stream")}}, hfetch.Messages[0].Headers)

	ack, err := stream.Ack(ctx, fetch.SubscriptionID, fetch.Messages[0].MessageID)
	require.NoError(t, err)
	require.Len(t, ack, 1)
	assert.Equal(t, fetch.Messages[0].MessageID, ack[0].MessageID)
	assert.Nil(t, ack[0].Error)

	nack, err := stream.Nack(ctx, hfetch.SubscriptionID, hfetch.Messages[0].MessageID)
	require.NoError(t, err)
	require.Len(t, nack, 1)
	assert.Nil(t, nack[0].Error)

	messages := make(chan client.Message, 2)
	subscription, err := stream.Subscribe(ctx, "sub", false, func(message client.Message) { messages <- message })
	require.NoError(t, err)
	assert.Equal(t, uint32(7), subscription.ID())
	select {
	case message := <-messages:
		assert.Equal(t, []byte("message-id"), message.MessageID)
		assert.Equal(t, []byte("subscription-message"), message.Payload)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, subscription.Close(ctx))
	require.NoError(t, subscription.Close(ctx))

	headered, err := stream.HSubscribe(ctx, "sub", true, func(message client.Message) { messages <- message })
	require.NoError(t, err)
	select {
	case message := <-messages:
		assert.Empty(t, message.MessageID)
		assert.Equal(t, []client.Header{{Key: []byte("content-type"), Value: []byte("text/plain")}}, message.Headers)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, headered.Close(ctx))

	select {
	case err := <-server.ping:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	require.NoError(t, stream.Close(ctx))
	require.NoError(t, stream.Close(ctx))
	require.ErrorIs(t, stream.Produce(ctx, "pub", []byte("closed")), session.ErrClosed)

	select {
	case err := <-server.done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestNativeClientLargeProduceFraming(t *testing.T) {
	server := startNativeTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := fujin.Dial(ctx, server.addr(), server.clientTLS, nil)
	require.NoError(t, err)
	defer conn.Close()
	stream, err := conn.Bind(ctx, "connector")
	require.NoError(t, err)

	payload := make([]byte, 1024*1024)
	require.NoError(t, stream.Produce(ctx, "pub", payload))
	require.NoError(t, stream.HProduce(ctx, "pub", payload, []client.Header{{Key: []byte("content-type"), Value: []byte("application/octet-stream")}}))
	require.NoError(t, stream.Close(ctx))
}

func TestOperationErrorFallbackText(t *testing.T) {
	assert.Equal(t, "reason", (&client.OperationError{Reason: "reason"}).Error())
	assert.Equal(t, "fujin operation failed with status 13", (&client.OperationError{Code: client.StatusInternal}).Error())
	assert.Equal(t, "", (*client.OperationError)(nil).Error())
}

func TestNativeSubscriptionIDRangeValidation(t *testing.T) {
	stream := &fujin.Stream{}
	err := stream.Unsubscribe(context.Background(), 256)
	assert.EqualError(t, err, "subscription ID 256 exceeds native range")
	_, err = stream.Ack(context.Background(), 256, []byte("message"))
	assert.EqualError(t, err, "subscription ID 256 exceeds native range")
}
