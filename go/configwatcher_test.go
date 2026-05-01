package proxy

import (
	"testing"
	"time"
)

func TestConfigWatcher_New_Good(t *testing.T) {
	// target symbol: New
	dir := t.TempDir()
	path := pathJoin(dir, "config.json")
	if err := writeFile(path, []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	watcher := NewConfigWatcher(path, func(*Config) {})
	if watcher == nil {
		t.Fatal("expected watcher")
	}
	if watcher.lastModifiedAt.IsZero() {
		t.Fatal("expected last modification time to be initialised from the file")
	}
}

func TestConfigwatcher_ConfigWatcher_Start_Good(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "config.json")
	initial := []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := writeFile(path, initial, 0o644); err != nil {
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
	if err := writeFile(path, updated, 0o644); err != nil {
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

// TestConfigWatcher_Start_Bad verifies a watcher with a nonexistent path does not panic
// and does not call the onChange callback.
//
//	watcher := proxy.NewConfigWatcher("/nonexistent/config.json", func(cfg *proxy.Config) {
//	    // never called
//	})
//	watcher.Start()
//	watcher.Stop()
func TestConfigwatcher_ConfigWatcher_Start_Bad(t *testing.T) {
	called := make(chan struct{}, 1)
	watcher := NewConfigWatcher("/nonexistent/path/config.json", func(*Config) {
		select {
		case called <- struct{}{}:
		default:
		}
	})
	if watcher == nil {
		t.Fatal("expected watcher even for a nonexistent path")
	}
	watcher.Start()
	defer watcher.Stop()

	select {
	case <-called:
		t.Fatal("expected no callback for nonexistent config file")
	case <-time.After(2 * time.Second):
		// expected: no update fired
	}
}

func TestConfigwatcher_ConfigWatcher_Start_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "config.json")
	initial := []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := writeFile(path, initial, 0o644); err != nil {
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

	if err := writeFile(path, initial, 0o644); err != nil {
		t.Fatalf("touch config file: %v", err)
	}

	select {
	case cfg := <-updates:
		if cfg == nil {
			t.Fatal("expected config update")
		}
		if got := cfg.Mode; got != "nicehash" {
			t.Fatalf("expected unchanged mode, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected watcher to reload touched config")
	}
}

// TestConfigWatcher_Stop_Good verifies that Stop closes a running watcher cleanly.
func TestConfigwatcher_ConfigWatcher_Stop_Good(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "config.json")
	initial := []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := writeFile(path, initial, 0o644); err != nil {
		t.Fatalf("write initial config file: %v", err)
	}

	watcher := NewConfigWatcher(path, func(*Config) {})
	if watcher == nil {
		t.Fatal("expected watcher")
	}
	watcher.Start()
	watcher.Stop()

	select {
	case <-watcher.stopCh:
	default:
		t.Fatal("expected watcher stop channel to be closed")
	}
}

// TestConfigWatcher_Stop_Bad verifies that a nil watcher is ignored.
func TestConfigwatcher_ConfigWatcher_Stop_Bad(t *testing.T) {
	var watcher *ConfigWatcher
	watcher.Stop()

	watcher = &ConfigWatcher{}
	watcher.Stop()
}

// TestConfigWatcher_Stop_Ugly verifies that Stop is idempotent across repeated calls.
func TestConfigwatcher_ConfigWatcher_Stop_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "config.json")
	initial := []byte(`{"mode":"nicehash","workers":"false","bind":[{"host":"127.0.0.1","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := writeFile(path, initial, 0o644); err != nil {
		t.Fatalf("write initial config file: %v", err)
	}

	watcher := NewConfigWatcher(path, func(*Config) {})
	if watcher == nil {
		t.Fatal("expected watcher")
	}

	watcher.Stop()
	watcher.Stop()

	watcher.Start()
	watcher.Stop()
	watcher.Stop()

	select {
	case <-watcher.stopCh:
	default:
		t.Fatal("expected watcher stop channel to remain closed after repeated stops")
	}
}
