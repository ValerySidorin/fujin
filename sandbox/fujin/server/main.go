package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/ValerySidorin/fujin/test"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	test.RunDefaultServerWithKafka3Brokers(ctx)
	<-ctx.Done()
	time.Sleep(1 * time.Second)
}
