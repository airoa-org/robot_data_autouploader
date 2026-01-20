package jobs

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// FileInfo represents information about a scanned file
type FileInfo struct {
	Name string `json:"name"` // Relative path from the scan directory
	Hash string `json:"hash"` // MD5 hash of the file
}

// FileScanOptions contains options for file scanning
type FileScanOptions struct {
	ExcludePatterns []string           // Patterns to exclude from scanning
	Logger          *zap.SugaredLogger // Logger for debug/warning messages
}

// ScanDirectoryForFiles scans the directory and returns a list of files with their MD5 hashes
func ScanDirectoryForFiles(dirPath string, options *FileScanOptions) ([]FileInfo, error) {
	var srcFiles []FileInfo

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if options != nil && options.Logger != nil {
				options.Logger.Debugw("Error accessing file during scan", "path", path, "error", err)
			}
			return nil // Continue walking, don't fail the entire scan
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check exclusion patterns if provided
		if options != nil && len(options.ExcludePatterns) > 0 {
			for _, pattern := range options.ExcludePatterns {
				matched, err := filepath.Match(pattern, filepath.Base(path))
				if err != nil {
					if options.Logger != nil {
						options.Logger.Debugw("Error matching exclusion pattern", "pattern", pattern, "file", path, "error", err)
					}
					continue
				}
				if matched {
					if options.Logger != nil {
						options.Logger.Debugw("Skipping excluded file during scan", "file", path, "pattern", pattern)
					}
					return nil
				}
			}
		}

		// Calculate relative path from the source directory
		relativePath, err := filepath.Rel(dirPath, path)
		if err != nil {
			if options != nil && options.Logger != nil {
				options.Logger.Debugw("Failed to get relative path", "path", path, "error", err)
			}
			relativePath = filepath.Base(path) // Fallback to just filename
		}

		// Calculate MD5 hash
		hash, err := calculateMD5(path)
		if err != nil {
			if options != nil && options.Logger != nil {
				options.Logger.Warnw("Failed to calculate MD5 hash for file", "path", path, "error", err)
			}
			return nil // Continue with other files
		}

		srcFiles = append(srcFiles, FileInfo{
			Name: relativePath,
			Hash: hash,
		})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %v", dirPath, err)
	}

	if options != nil && options.Logger != nil {
		options.Logger.Debugw("Scanned directory for files", "dirPath", dirPath, "fileCount", len(srcFiles))
	}
	return srcFiles, nil
}

// calculateMD5 calculates the MD5 hash of a file
func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
