package main

import (
	"testing"
)

func TestDoMain(t *testing.T) {
}

func TestDoVersion(t *testing.T) {
	result := doVersion()
	if result != 0 {
		t.Errorf("doVersion() = %d, want 0", result)
	}
}

func TestDoDumpConfigSuccess(t *testing.T) {
	opts := makeTestOpts("")
	result := doDumpConfig(opts)
	if result != 0 {
		t.Errorf("doDumpConfig() = %d, want 0", result)
	}
}

func TestDoDumpConfigError(t *testing.T) {
	opts := makeTestOpts("/nonexistent/path/to/config.json")
	result := doDumpConfig(opts)
	if result != 1 {
		t.Errorf("doDumpConfig() with error = %d, want 1", result)
	}
}
