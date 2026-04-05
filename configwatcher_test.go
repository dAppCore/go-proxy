package proxy

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigWatcher_New_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`), 0o644); err != nil {
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

func TestConfigWatcher_Start_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	initial := []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write initial config file: %v", err)
	}

	updates := make(chan *Config, 1)
	watcher := NewConfigWatcher(path, func(cfg *Config) {
		select {
		case updates <- cfg:
		default:
		}
	})
	if watcher == nil {
		t.Fatal("expected watcher")
	}
	watcher.Start()
	defer watcher.Stop()

	updated := []byte(`{"mode":"simple","workers":"user","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := os.WriteFile(path, updated, 0o644); err != nil {
		t.Fatalf("write updated config file: %v", err)
	}

	select {
	case cfg := <-updates:
		if cfg == nil {
			t.Fatal("expected config update")
		}
		if got := cfg.Mode; got != "simple" {
			t.Fatalf("expected updated mode, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected watcher to reload updated config")
	}
}
