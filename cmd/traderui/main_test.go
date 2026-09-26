package main

import (
	"path/filepath"
	"testing"
)

func TestFrozenPipelineFailureDoesNotRequireConsoleShutdown(t *testing.T) {
	pipeline, err := loadFrozenForConsole(t.TempDir())
	if err == nil || pipeline != nil {
		t.Fatalf("pipeline=%v err=%v", pipeline, err)
	}
}

func TestFrozenPipelineStillLoadsWhenArtifactsAreValid(t *testing.T) {
	pipeline, err := loadFrozenForConsole(filepath.Join("..", ".."))
	if err != nil || pipeline == nil {
		t.Fatalf("pipeline=%v err=%v", pipeline, err)
	}
}
