package proxy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfig_LoadConfig_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"mode":"nicehash","workers":"rig-id","bind":[{"host":"0.0.0.0","port":3333}],"pools":[{"url":"pool.example:3333","enabled":true}]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("expected config file write to succeed: %v", err)
	}

	cfg, result := LoadConfig(path)
	if !result.OK {
		t.Fatalf("expected load to succeed, got error: %v", result.Error)
	}
	if cfg == nil {
		t.Fatal("expected config to be returned")
	}
	if got := cfg.Mode; got != "nicehash" {
		t.Fatalf("expected mode to round-trip, got %q", got)
	}
	if got := cfg.configPath; got != path {
		t.Fatalf("expected config path to be recorded, got %q", got)
	}
}

func TestConfig_LoadConfig_Bad(t *testing.T) {
	t.Run("missing_file", func(t *testing.T) {
		cfg, result := LoadConfig(filepath.Join(t.TempDir(), "missing.json"))
		if result.OK {
			t.Fatalf("expected missing config file to fail, got cfg=%+v", cfg)
		}
		if cfg != nil {
			t.Fatalf("expected no config on read failure, got %+v", cfg)
		}
	})

	t.Run("malformed_json", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		data := []byte(`{"mode":"nicehash","workers":"rig-id",`)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("expected config file write to succeed: %v", err)
		}

		cfg, result := LoadConfig(path)
		if result.OK {
			t.Fatalf("expected malformed config file to fail, got cfg=%+v", cfg)
		}
		if cfg != nil {
			t.Fatalf("expected no config on parse failure, got %+v", cfg)
		}
	})
}

func TestConfig_LoadConfig_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"mode":"invalid","workers":"rig-id","bind":[],"pools":[]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("expected config file write to succeed: %v", err)
	}

	cfg, result := LoadConfig(path)
	if !result.OK {
		t.Fatalf("expected syntactically valid JSON to load, got error: %v", result.Error)
	}
	if cfg == nil {
		t.Fatal("expected config to be returned")
	}
	if got := cfg.Mode; got != "invalid" {
		t.Fatalf("expected invalid mode value to be preserved, got %q", got)
	}
	if validation := cfg.Validate(); validation.OK {
		t.Fatal("expected semantic validation to fail separately from loading")
	}
}
