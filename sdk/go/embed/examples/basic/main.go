package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	fujin "github.com/fujin-io/fujin/sdk/go/embed"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	libraryPath := os.Getenv("FUJIN_LIBRARY_PATH")
	if libraryPath == "" {
		return errors.New("FUJIN_LIBRARY_PATH must point to the generated Fujin library")
	}

	library, err := fujin.Open(libraryPath)
	if err != nil {
		return err
	}
	defer library.Close()

	application, err := library.Start(context.Background(), fujin.Options{
		Config: &fujin.RuntimeConfig{
			Fujin: fujin.FujinConfig{Transports: []fujin.TransportConfig{{
				Type:     "tcp",
				Settings: map[string]any{"addr": "127.0.0.1:0"},
			}}},
			GRPC:       &fujin.GRPCConfig{Enabled: false},
			Connectors: map[string]fujin.ConnectorConfig{},
		},
	})
	if err != nil {
		return err
	}
	defer application.Close()

	endpoints, err := application.Endpoints()
	if err != nil {
		return err
	}
	fmt.Printf("Fujin ready: %+v\n", endpoints)

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	select {
	case <-signalContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return application.Shutdown(shutdownContext)
	case <-application.Done():
		return application.Wait()
	}
}
