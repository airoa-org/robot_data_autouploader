package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Config represents the application configuration
type Config struct {
	Daemon  DaemonConfig  `mapstructure:"daemon"`
	Storage StorageConfig `mapstructure:"storage"`
	USB     USBConfig     `mapstructure:"usb"`
	Jobs    JobsConfig    `mapstructure:"jobs"`
	Copy    CopyConfig    `mapstructure:"copy"`
	Upload  UploadConfig  `mapstructure:"upload"`
}

// DaemonConfig contains settings for the daemon service
type DaemonConfig struct {
	DatabasePath string `mapstructure:"database_path"`
}

// StorageConfig contains settings for storage (S3 and local)
type StorageConfig struct {
	S3    S3Config    `mapstructure:"s3"`
	Local LocalConfig `mapstructure:"local"`
}

// S3Config contains S3 storage settings
type S3Config struct {
	Bucket     string `mapstructure:"bucket"`
	Endpoint   string `mapstructure:"endpoint"`
	Region     string `mapstructure:"region"`
	AccessKey  string `mapstructure:"access_key"`
	SecretKey  string `mapstructure:"secret_key"`
	UploadPath string `mapstructure:"upload_path"`
}

// LocalConfig contains local storage settings
type LocalConfig struct {
	StagingDir              string   `mapstructure:"staging_dir"`
	RetentionPolicyOnUpload string   `mapstructure:"retention_policy_on_upload"`
	RetentionPolicyOnCopy   string   `mapstructure:"retention_policy_on_copy"`
	SourcePatterns          []string `mapstructure:"source_patterns"`
}

// USBConfig contains USB detection settings
type USBConfig struct {
	TargetMountPoint string   `mapstructure:"target_mount_point"`
	TargetDirectory  string   `mapstructure:"target_directory"`
	IgnoredPatterns  []string `mapstructure:"ignored_patterns"`
	ScanIntervalMs   int      `mapstructure:"scan_interval_ms"`
}

// JobsConfig contains job management settings
type JobsConfig struct {
	DirectUpload bool `mapstructure:"direct_upload"`
	MaxRetries   int  `mapstructure:"max_retries"`
}

// CopyConfig contains file copy settings
type CopyConfig struct {
	ExcludePatterns   []string `mapstructure:"exclude_patterns"`
	MinFreeSpaceRatio float64  `mapstructure:"min_free_space_ratio"` // Minimum ratio of free space to keep after copy (e.g. 0.1 for 10%)
}

// UploadConfig contains S3 upload settings
type UploadConfig struct {
	Parallelism     int      `mapstructure:"parallelism"`
	ChunkSizeMb     int      `mapstructure:"chunk_size_mb"`
	ThrottleMbps    int      `mapstructure:"throttle_mbps"`
	AllowedPatterns []string `mapstructure:"allowed_patterns"` // Only upload files matching these patterns (empty = allow all)
}

// LoadConfig loads the configuration from the specified file
func LoadConfig(configPath string) (*Config, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	v := viper.New()
	v.SetConfigType("yaml")

	// Set default values
	setDefaultValues(v)

	// If configPath is provided, use it
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Look for config in standard locations
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.config/autoloader")
		v.AddConfigPath("/etc/autoloader")
		v.SetConfigName("config")
	}

	// Read environment variables
	v.SetEnvPrefix("AUTOLOADER")
	v.AutomaticEnv()

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			sugar.Warnf("Config file not found, using defaults: %v", err)
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Parse the config
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Check for unused configuration keys
	if err := checkUnusedKeys(v); err != nil {
		return nil, err
	}

	// Expand environment variables and home directory in paths
	expandPaths(&config)

	return &config, nil
}

// expandPaths expands environment variables and ~ in file paths
func expandPaths(config *Config) {
	config.Daemon.DatabasePath = expandPath(config.Daemon.DatabasePath)
	config.Storage.Local.StagingDir = expandPath(config.Storage.Local.StagingDir)

	// Expand source patterns
	for i, pattern := range config.Storage.Local.SourcePatterns {
		config.Storage.Local.SourcePatterns[i] = expandPath(pattern)
	}
}

// expandPath expands environment variables and ~ in a file path
func expandPath(path string) string {
	// Expand environment variables
	expanded := os.ExpandEnv(path)

	// Expand home directory
	if len(expanded) > 0 && expanded[0] == '~' {
		home, err := os.UserHomeDir()
		if err == nil {
			expanded = filepath.Join(home, expanded[1:])
		}
	}

	return expanded
}

// checkUnusedKeys validates that all keys in the config file are actually used
func checkUnusedKeys(v *viper.Viper) error {
	// Get all keys that were loaded from the config file
	allKeys := v.AllKeys()

	// Get all valid keys based on our struct definitions
	validKeys := getValidConfigKeys()

	// Find any keys that are present but not valid
	var unusedKeys []string
	for _, key := range allKeys {
		if !validKeys[key] {
			unusedKeys = append(unusedKeys, key)
		}
	}

	// Return error if there are unused keys
	if len(unusedKeys) > 0 {
		return fmt.Errorf("unused configuration keys found: %s", strings.Join(unusedKeys, ", "))
	}

	return nil
}

// getValidConfigKeys returns all valid configuration keys based on struct tags
func getValidConfigKeys() map[string]bool {
	validKeys := make(map[string]bool)

	// Add all valid keys from the Config struct and its nested structs
	addStructKeys(validKeys, reflect.TypeOf(Config{}), "")

	return validKeys
}

// addStructKeys recursively adds all mapstructure keys from a struct type
func addStructKeys(validKeys map[string]bool, t reflect.Type, prefix string) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("mapstructure")

		if tag == "" || tag == "-" {
			continue
		}

		// Build the full key path
		var fullKey string
		if prefix == "" {
			fullKey = tag
		} else {
			fullKey = prefix + "." + tag
		}

		// Add this key as valid
		validKeys[fullKey] = true

		// If this field is a struct, recursively add its keys
		fieldType := field.Type
		if fieldType.Kind() == reflect.Struct {
			addStructKeys(validKeys, fieldType, fullKey)
		}
	}
}

// setDefaultValues sets the default configuration values
func setDefaultValues(v *viper.Viper) {
	// Daemon defaults
	v.SetDefault("daemon.database_path", "~/.autoloader/jobs.db")

	// Storage defaults
	v.SetDefault("storage.s3.access_key", "") // Re-add default
	v.SetDefault("storage.s3.secret_key", "") // Re-add default
	v.SetDefault("storage.s3.region", "us-west-1")
	v.SetDefault("storage.local.staging_dir", "~/.autoloader/staging")
	v.SetDefault("storage.local.retention_policy_on_upload", "")
	v.SetDefault("storage.local.retention_policy_on_copy", "")

	v.SetDefault("storage.local.source_patterns", []string{
		"/opt/data/{{.Dir}}/rosbags",
		"/opt/data/{{.Dir}}",
	})

	// USB defaults
	v.SetDefault("usb.ignored_patterns", []string{"SYSTEM*", ".*"})
	v.SetDefault("usb.scan_interval_ms", 500)
	v.SetDefault("usb.target_mount_point", "/storage")
	v.SetDefault("usb.target_directory", "")

	// Jobs defaults
	v.SetDefault("jobs.direct_upload", false)
	v.SetDefault("jobs.max_retries", 3)

	// Copy defaults
	v.SetDefault("copy.exclude_patterns", []string{"*.tmp", "*.DS_Store"})
	v.SetDefault("copy.min_free_space_ratio", 0.05) // Default: keep at least 5% free space

	// Upload defaults
	v.SetDefault("upload.parallelism", 1)
	v.SetDefault("upload.throttle_mbps", 50)
	v.SetDefault("upload.chunk_size_mb", 8)
	v.SetDefault("upload.allowed_patterns", []string{}) // Empty = allow all files
}
