//go:build amqp10

package amqp10_test

import (
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/ValerySidorin/fujin/public/connectors/impl/amqp10"
	"github.com/stretchr/testify/assert"
)

func TestGetSetDeliveryID(t *testing.T) {
	msg := &amqp.Message{}
	amqp10.SetDeliveryId(msg, 100)
	deliveryID := amqp10.GetDeliveryId(msg)

	assert.EqualValues(t, 100, deliveryID)
}
