package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnusedConfigKeys(t *testing.T) {
	// Create a temporary config file with unused keys
	configContent := `
daemon:
  database_path: "/tmp/test.db"
storage:
  s3:
    bucket: "test-bucket"
    region: "us-west-1"
  local:
    staging_dir: "/tmp/staging"
usb:
  scan_interval_ms: 1000
invalid_key: "this should cause an error"
another_invalid: 123
nested:
  invalid_nested: "also invalid"
`

	// Create temporary file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yaml")

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Test loading config with unused keys - should fail
	_, err = LoadConfig(configPath)
	if err == nil {
		t.Fatal("Expected error for unused config keys, but got none")
	}

	// Check that the error mentions the unused keys
	errorMsg := err.Error()
	if !strings.Contains(errorMsg, "unused configuration keys") {
		t.Errorf("Expected error about unused keys, got: %v", err)
	}

	// Check that specific invalid keys are mentioned
	expectedInvalidKeys := []string{"invalid_key", "another_invalid", "nested.invalid_nested"}
	for _, key := range expectedInvalidKeys {
		if !strings.Contains(errorMsg, key) {
			t.Errorf("Expected error to mention invalid key '%s', but it didn't. Error: %v", key, err)
		}
	}
}

func TestValidConfigKeys(t *testing.T) {
	// Create a config file with only valid keys
	configContent := `
daemon:
  database_path: "/tmp/test.db"
storage:
  s3:
    bucket: "test-bucket"
    region: "us-west-1"
    access_key: "test-key"
    secret_key: "test-secret"
    endpoint: "http://localhost:9000"
    upload_path: "uploads"
  local:
    staging_dir: "/tmp/staging"
    retention_policy_on_upload: "delete"
    retention_policy_on_copy: "keep"
    source_patterns:
      - "/opt/data/*/rosbags"
usb:
  target_mount_point: "/storage"
  target_directory: "data"
  ignored_patterns:
    - "SYSTEM*"
    - ".*"
  scan_interval_ms: 1000
jobs:
  direct_upload: true
  max_retries: 5
copy:
  exclude_patterns:
    - "*.tmp"
  min_free_space_ratio: 0.1
upload:
  parallelism: 2
  chunk_size_mb: 16
  throttle_mbps: 100
`

	// Create temporary file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "valid_config.yaml")

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Test loading config with only valid keys - should succeed
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Expected no error for valid config, but got: %v", err)
	}

	// Verify some config values were loaded correctly
	if config.Daemon.DatabasePath != "/tmp/test.db" {
		t.Errorf("Expected database_path to be '/tmp/test.db', got: %s", config.Daemon.DatabasePath)
	}

	if config.Storage.S3.Bucket != "test-bucket" {
		t.Errorf("Expected S3 bucket to be 'test-bucket', got: %s", config.Storage.S3.Bucket)
	}

	if config.USB.ScanIntervalMs != 1000 {
		t.Errorf("Expected scan interval to be 1000, got: %d", config.USB.ScanIntervalMs)
	}
}

func TestGetValidConfigKeys(t *testing.T) {
	validKeys := getValidConfigKeys()

	// Test some expected valid keys
	expectedKeys := []string{
		"daemon.database_path",
		"storage.s3.bucket",
		"storage.s3.region",
		"storage.local.staging_dir",
		"usb.target_mount_point",
		"usb.scan_interval_ms",
		"jobs.direct_upload",
		"copy.exclude_patterns",
		"upload.parallelism",
	}

	for _, key := range expectedKeys {
		if !validKeys[key] {
			t.Errorf("Expected key '%s' to be valid, but it wasn't found", key)
		}
	}

	// Test that we have a reasonable number of keys
	if len(validKeys) < 15 {
		t.Errorf("Expected at least 15 valid keys, got %d", len(validKeys))
	}
}
