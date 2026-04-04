package proxy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigWatcher_New_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"mode":"nicehash"}`), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	watcher := NewConfigWatcher(path, func(*Config) {})
	if watcher == nil {
		t.Fatal("expected watcher")
	}
	if watcher.lastMod.IsZero() {
		t.Fatal("expected last modification time to be initialised from the file")
	}
}
