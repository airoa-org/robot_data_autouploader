package lineage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MetadataInfo contains extracted metadata information
type MetadataInfo struct {
	RobotID  string
	Location string
	Version  string
}

// Entity represents entity structure for v1.1+
type Entity struct {
	Role string `json:"role"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// Context represents context structure for v1.1+
type Context struct {
	Entities []Entity `json:"entities"`
}

// MetadataV1_0 represents metadata structure for v1.0 and below
type MetadataV1_0 struct {
	Version      string `json:"version"`
	HSRID        string `json:"hsr_id"`
	LocationName string `json:"location_name"`
}

// MetadataV1_1Plus represents metadata structure for v1.1+
type MetadataV1_1Plus struct {
	Version string  `json:"version"`
	Context Context `json:"context"`
}

// ReadMetadata reads metadata file and extracts required information
func ReadMetadata(metaFilePath string) (*MetadataInfo, error) {
	data, err := os.ReadFile(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file %s: %w", metaFilePath, err)
	}

	var versionCheck struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &versionCheck); err != nil {
		return nil, fmt.Errorf("failed to parse version from metadata: %w", err)
	}

	version := versionCheck.Version

	if compareVersion(version, "1.1") < 0 {
		return parseMetadataV1_0(data, version)
	} else {
		return parseMetadataV1_1Plus(data, version)
	}
}

// parseMetadataV1_0 parses v1.0 and below metadata format
func parseMetadataV1_0(data []byte, version string) (*MetadataInfo, error) {
	var meta MetadataV1_0
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse v1.0 metadata: %w", err)
	}

	return &MetadataInfo{
		RobotID:  meta.HSRID,
		Location: meta.LocationName,
		Version:  version,
	}, nil
}

// parseMetadataV1_1Plus parses v1.1+ metadata format
func parseMetadataV1_1Plus(data []byte, version string) (*MetadataInfo, error) {
	var meta MetadataV1_1Plus
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse v1.1+ metadata: %w", err)
	}

	var robotID, location string

	for _, entity := range meta.Context.Entities {
		switch entity.Role {
		case "robot":
			robotID = entity.ID
		case "location":
			location = entity.Name
		}
	}

	if robotID == "" {
		return nil, fmt.Errorf("robot ID not found in metadata")
	}
	if location == "" {
		return nil, fmt.Errorf("location not found in metadata")
	}

	return &MetadataInfo{
		RobotID:  robotID,
		Location: location,
		Version:  version,
	}, nil
}

// compareVersion compares version strings
// Returns: negative if a < b, zero if a == b, positive if a > b
func compareVersion(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	maxLen := len(aParts)
	if len(bParts) > maxLen {
		maxLen = len(bParts)
	}

	for i := 0; i < maxLen; i++ {
		var aNum, bNum int
		if i < len(aParts) {
			fmt.Sscanf(aParts[i], "%d", &aNum)
		}
		if i < len(bParts) {
			fmt.Sscanf(bParts[i], "%d", &bNum)
		}

		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
	}

	return 0
}

// FindMetadataFile searches for meta.json file in the specified directory
func FindMetadataFile(dir string) (string, error) {
	metaPath := filepath.Join(dir, "meta.json")
	if _, err := os.ReadFile(metaPath); err == nil {
		return metaPath, nil
	}

	return "", fmt.Errorf("meta.json file not found in directory: %s", dir)
}
