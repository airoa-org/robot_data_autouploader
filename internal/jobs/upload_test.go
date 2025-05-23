package jobs

import (
	"testing"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"go.uber.org/zap"
)

func TestIsFileAllowed(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	tests := []struct {
		name            string
		allowedPatterns []string
		filePath        string
		expected        bool
	}{
		{
			name:            "empty patterns allows all files",
			allowedPatterns: []string{},
			filePath:        "/path/to/any_file.txt",
			expected:        true,
		},
		{
			name:            "nil patterns allows all files",
			allowedPatterns: nil,
			filePath:        "/path/to/any_file.txt",
			expected:        true,
		},
		{
			name:            "matching pattern allows file",
			allowedPatterns: []string{"*.bag"},
			filePath:        "/path/to/data.bag",
			expected:        true,
		},
		{
			name:            "non-matching pattern blocks file",
			allowedPatterns: []string{"*.bag"},
			filePath:        "/path/to/data.txt",
			expected:        false,
		},
		{
			name:            "multiple patterns - first matches",
			allowedPatterns: []string{"*.bag", "*.log"},
			filePath:        "/path/to/data.bag",
			expected:        true,
		},
		{
			name:            "multiple patterns - second matches",
			allowedPatterns: []string{"*.bag", "*.log"},
			filePath:        "/path/to/debug.log",
			expected:        true,
		},
		{
			name:            "multiple patterns - none match",
			allowedPatterns: []string{"*.bag", "*.log"},
			filePath:        "/path/to/data.txt",
			expected:        false,
		},
		{
			name:            "wildcard pattern matches prefix",
			allowedPatterns: []string{"data_*"},
			filePath:        "/path/to/data_2024.csv",
			expected:        true,
		},
		{
			name:            "wildcard pattern doesn't match different prefix",
			allowedPatterns: []string{"data_*"},
			filePath:        "/path/to/backup_2024.csv",
			expected:        false,
		},
		{
			name:            "exact filename match",
			allowedPatterns: []string{"config.yaml"},
			filePath:        "/path/to/config.yaml",
			expected:        true,
		},
		{
			name:            "exact filename no match",
			allowedPatterns: []string{"config.yaml"},
			filePath:        "/path/to/settings.yaml",
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal config with the test patterns
			config := &appconfig.Config{
				Upload: appconfig.UploadConfig{
					AllowedPatterns: tt.allowedPatterns,
				},
			}

			// Create a minimal UploadWorker with just the config and logger
			worker := &UploadWorker{
				config: config,
				logger: sugar,
			}

			// Test the isFileAllowed function
			result := worker.isFileAllowed(tt.filePath)

			if result != tt.expected {
				t.Errorf("isFileAllowed(%q) with patterns %v = %v, want %v",
					tt.filePath, tt.allowedPatterns, result, tt.expected)
			}
		})
	}
}

func TestIsFileAllowedWithComplexPatterns(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Test with more complex real-world patterns
	config := &appconfig.Config{
		Upload: appconfig.UploadConfig{
			AllowedPatterns: []string{
				"*.bag",       // ROS bag files
				"*.log",       // Log files
				"data_*.csv",  // Data CSV files with prefix
				"config.yaml", // Specific config file
			},
		},
	}

	worker := &UploadWorker{
		config: config,
		logger: sugar,
	}

	testCases := []struct {
		filePath string
		expected bool
	}{
		{"/robot/recordings/session1.bag", true},
		{"/robot/logs/debug.log", true},
		{"/robot/data/data_sensors.csv", true},
		{"/robot/config/config.yaml", true},
		{"/robot/temp/temp.txt", false},
		{"/robot/backup/backup.zip", false},
		{"/robot/data/sensors.csv", false}, // doesn't match data_* pattern
	}

	for _, tc := range testCases {
		t.Run(tc.filePath, func(t *testing.T) {
			result := worker.isFileAllowed(tc.filePath)
			if result != tc.expected {
				t.Errorf("isFileAllowed(%q) = %v, want %v", tc.filePath, result, tc.expected)
			}
		})
	}
}
