package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}

	for {
		if err := nc.Publish("my_subject", []byte("hello")); err != nil {
			log.Fatal(err)
		}
		// time.Sleep(1 * time.Millisecond)
		// fmt.Println("produced")
	}
}
