package main

import (
	"path/filepath"
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

func TestNormalizeReplacementResolvesLocalPath(t *testing.T) {
	base := filepath.Join(string(filepath.Separator), "workspace", "fujin")
	replacement, err := normalizeReplacement("github.com/fujin-io/fujin-control-plane=../fujin-control-plane", base)
	if err != nil {
		t.Fatal(err)
	}
	want := "github.com/fujin-io/fujin-control-plane=" + filepath.Join(string(filepath.Separator), "workspace", "fujin-control-plane")
	if replacement != want {
		t.Fatalf("replacement = %q, want %q", replacement, want)
	}
}

func TestNormalizeReplacementRejectsMissingPath(t *testing.T) {
	if _, err := normalizeReplacement("github.com/fujin-io/plugin", "."); err == nil {
		t.Fatal("expected invalid replacement error")
	}
}

func TestValidatePluginRequirementsForZeroMQPebbe(t *testing.T) {
	const plugin = "github.com/fujin-io/fujin/public/plugins/connector/zeromq/pebbe"
	if err := validatePluginRequirements([]string{plugin}, "fujin,zeromq_pebbe", false); err == nil || !strings.Contains(err.Error(), "requires -cgo") {
		t.Fatalf("expected -cgo requirement, got %v", err)
	}
	if err := validatePluginRequirements([]string{plugin}, "fujin", true); err == nil || !strings.Contains(err.Error(), "requires build tag zeromq_pebbe") {
		t.Fatalf("expected build-tag requirement, got %v", err)
	}
	if err := validatePluginRequirements([]string{plugin}, "fujin,zeromq_pebbe", true); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePluginRequirementsLeavesOrdinaryConnectorsUnchanged(t *testing.T) {
	if err := validatePluginRequirements([]string{"github.com/fujin-io/fujin/public/plugins/connector/all"}, "fujin", false); err != nil {
		t.Fatal(err)
	}
}
