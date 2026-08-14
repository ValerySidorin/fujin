package main

import (
	"strings"
	"testing"
)

func TestGenerateMainPropagatesVersionToService(t *testing.T) {
	generated := generateMain(pluginsByType{})
	assignment := strings.Index(generated, "service.Version = Version")
	run := strings.Index(generated, "service.RunCLI(ctx)")
	if assignment < 0 {
		t.Fatal("generated main does not propagate Version to service.Version")
	}
	if run < 0 {
		t.Fatal("generated main does not call service.RunCLI")
	}
	if assignment > run {
		t.Fatal("generated main propagates Version after service.RunCLI")
	}
}
