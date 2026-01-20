package lineage

import (
	"os"
	"path/filepath"
	"testing"
)

// createTestMetadataV1_0 creates test v1.0 metadata file
func createTestMetadataV1_0(filePath string) error {
	testData := `{
	"bag_path": "data.bag",
	"hsr_id": "hsrc039",
	"version": "1.0",
	"location_name": "tmc",
	"interface": "hsr_leader_teleop",
	"instructions": [
		["Take a coffee can from the shelf"]
	],
	"segments": [
		{
			"start_time": 1749521771.101148,
			"end_time": 1749521784.2010143,
			"instructions_index": 0,
			"has_suboptimal": false,
			"is_directed": true
		}
	]
}`
	return os.WriteFile(filePath, []byte(testData), 0644)
}

// createTestMetadataV1_3 creates test v1.3 metadata file
func createTestMetadataV1_3(filePath string) error {
	testData := `{
	"uuid": "1fd580ef-d2d9-47c2-981d-98f1812bac8a",
	"version": "1.3",
	"files": [
		{
			"type": "rosbag",
			"name": "data.bag"
		}
	],
	"context": {
		"entities": [
			{
			"role": "robot",
			"id": "hsrc039"
			},
			{
			"role": "location",
			"name": "tmc"
			}
		]
	},
	"run": {
		"total_time_s": 491.59995579719543,
		"instructions": [
			{
			"idx": 0,
			"text": ["Take a coffee can from the shelf"]
			}
		]
	}
}`
	return os.WriteFile(filePath, []byte(testData), 0644)
}

func TestReadMetadataV1_0(t *testing.T) {
	testDir := t.TempDir()

	metaPath := filepath.Join(testDir, "meta.json")
	err := createTestMetadataV1_0(metaPath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	info, err := ReadMetadata(metaPath)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}
	expectedRobotID := "hsrc039"
	expectedLocation := "tmc"
	expectedVersion := "1.0"

	if info.RobotID != expectedRobotID {
		t.Errorf("Expected RobotID %s, got %s", expectedRobotID, info.RobotID)
	}
	if info.Location != expectedLocation {
		t.Errorf("Expected Location %s, got %s", expectedLocation, info.Location)
	}
	if info.Version != expectedVersion {
		t.Errorf("Expected Version %s, got %s", expectedVersion, info.Version)
	}
}

func TestReadMetadataV1_3(t *testing.T) {
	testDir := t.TempDir()

	metaPath := filepath.Join(testDir, "meta.json")
	err := createTestMetadataV1_3(metaPath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	info, err := ReadMetadata(metaPath)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}
	expectedRobotID := "hsrc039"
	expectedLocation := "tmc"
	expectedVersion := "1.3"

	if info.RobotID != expectedRobotID {
		t.Errorf("Expected RobotID %s, got %s", expectedRobotID, info.RobotID)
	}
	if info.Location != expectedLocation {
		t.Errorf("Expected Location %s, got %s", expectedLocation, info.Location)
	}
	if info.Version != expectedVersion {
		t.Errorf("Expected Version %s, got %s", expectedVersion, info.Version)
	}
}

func TestReadMetadataFileNotFound(t *testing.T) {
	_, err := ReadMetadata("/non/existent/meta.json")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestReadMetadataInvalidJSON(t *testing.T) {
	testDir := t.TempDir()

	metaPath := filepath.Join(testDir, "meta.json")
	invalidJSON := `{
	"version": "1.0",
	"hsr_id": "hsrc039"
  // invalid JSON syntax
}`
	err := os.WriteFile(metaPath, []byte(invalidJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid JSON file: %v", err)
	}

	_, err = ReadMetadata(metaPath)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestReadMetadataMissingRobotID(t *testing.T) {
	testDir := t.TempDir()

	metaPath := filepath.Join(testDir, "meta.json")
	testData := `{
	"version": "1.3",
	"context": {
		"entities": [
			{
			"role": "location",
			"name": "tmc"
			}
		]
	}
}`
	err := os.WriteFile(metaPath, []byte(testData), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err = ReadMetadata(metaPath)
	if err == nil {
		t.Error("Expected error for missing robot ID, got nil")
	}
}

func TestFindMetadataFile(t *testing.T) {
	testDir := t.TempDir()

	metaPath := filepath.Join(testDir, "meta.json")
	err := createTestMetadataV1_0(metaPath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	foundPath, err := FindMetadataFile(testDir)
	if err != nil {
		t.Fatalf("Failed to find metadata file: %v", err)
	}
	if foundPath != metaPath {
		t.Errorf("Expected path %s, got %s", metaPath, foundPath)
	}
}

func TestFindMetadataFileNotFound(t *testing.T) {
	testDir := t.TempDir()

	_, err := FindMetadataFile(testDir)
	if err == nil {
		t.Error("Expected error for missing meta.json file, got nil")
	}
}

func TestCompareVersion(t *testing.T) {
	testCases := []struct {
		a        string
		b        string
		expected int
	}{
		{"1.0", "1.1", -1},     // 1.0 < 1.1
		{"1.3", "1.1", 1},      // 1.3 > 1.1
		{"1.0", "1.0", 0},      // 1.0 == 1.0
		{"2.0", "1.9", 1},      // 2.0 > 1.9
		{"1.0.0", "1.0", 0},    // 1.0.0 == 1.0
		{"1.2.3", "1.2.4", -1}, // 1.2.3 < 1.2.4
	}

	for _, tc := range testCases {
		result := compareVersion(tc.a, tc.b)

		var resultSign int
		if result < 0 {
			resultSign = -1
		} else if result > 0 {
			resultSign = 1
		} else {
			resultSign = 0
		}

		if resultSign != tc.expected {
			t.Errorf("compareVersion(%s, %s): expected %d, got %d", tc.a, tc.b, tc.expected, resultSign)
		}
	}
}
