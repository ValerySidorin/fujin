package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/runtimeconfig"
)

type connectorRuntimeController = runtimeconfig.Controller

func newConnectorRuntimeController(
	reloader runtimeconfig.Reloader,
	initial configurator.ConnectorSnapshot,
) (*connectorRuntimeController, error) {
	return runtimeconfig.NewController(reloader, initial, Version)
}

// connectorBootstrapSnapshot binds optional source metadata to the configuration loaded into Fujin.
func connectorBootstrapSnapshot(
	loader configurator.Configurator,
	connectors connectorconfig.ConnectorsConfig,
) (configurator.ConnectorSnapshot, error) {
	loaded, loadedDigest, err := snapshotConnectorConfig(connectors)
	if err != nil {
		return configurator.ConnectorSnapshot{}, err
	}
	initial := configurator.ConnectorSnapshot{Connectors: loaded}
	source, ok := loader.(configurator.ConnectorBootstrapSnapshot)
	if !ok {
		return initial, nil
	}
	declared, ok := source.InitialConnectorSnapshot()
	if !ok {
		return initial, nil
	}
	declaredConnectors, declaredDigest, err := snapshotConnectorConfig(declared.Connectors)
	if err != nil {
		return configurator.ConnectorSnapshot{}, fmt.Errorf("snapshot declared bootstrap connectors: %w", err)
	}
	if declaredDigest != loadedDigest {
		return configurator.ConnectorSnapshot{}, errors.New("configurator bootstrap snapshot does not match loaded connectors")
	}
	declared.Connectors = declaredConnectors
	return declared, nil
}

func startConnectorWatcher(
	ctx context.Context,
	loader configurator.Configurator,
	controller *connectorRuntimeController,
) <-chan error {
	watcher, ok := loader.(configurator.ConnectorWatcher)
	if !ok {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		runtime := newConnectorRuntimeQueue(ctx, controller)
		defer runtime.Close()
		done <- watcher.WatchConnectors(ctx, runtime)
	}()
	return done
}

func reloadConnectorsFromConfigurator(
	ctx context.Context,
	loader configurator.Configurator,
	controller *connectorRuntimeController,
	revision *uint64,
) error {
	if _, watcherOwnsConnectors := loader.(configurator.ConnectorWatcher); watcherOwnsConnectors {
		return nil
	}
	var next Config
	if err := loader.Load(ctx, &next); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	*revision = *revision + 1
	result := controller.Apply(ctx, configurator.ConnectorSnapshot{
		Revision:   *revision,
		Connectors: next.Connectors,
	})
	return result.Err
}

func snapshotConnectorConfig(
	connectors connectorconfig.ConnectorsConfig,
) (connectorconfig.ConnectorsConfig, [sha256.Size]byte, error) {
	return runtimeconfig.CloneSnapshot(connectors)
}
