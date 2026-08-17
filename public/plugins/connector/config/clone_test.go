package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneConnectorConfigCopiesContainersAndPreservesOpaqueLeaves(t *testing.T) {
	signal := make(chan uint64, 1)
	settings := map[string]any{
		"nested": map[string]any{"values": []any{"one", "two"}},
		"signal": signal,
	}
	cloned, err := CloneConnectorConfig(ConnectorConfig{Type: "test", Settings: settings})
	require.NoError(t, err)
	clonedSettings := cloned.Settings.(map[string]any)
	clonedNested := clonedSettings["nested"].(map[string]any)
	clonedValues := clonedNested["values"].([]any)

	settings["new"] = true
	settings["nested"].(map[string]any)["values"].([]any)[0] = "changed"
	assert.NotContains(t, clonedSettings, "new")
	assert.Equal(t, "one", clonedValues[0])
	assert.Equal(t, signal, clonedSettings["signal"])
}
