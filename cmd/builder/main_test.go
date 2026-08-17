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
