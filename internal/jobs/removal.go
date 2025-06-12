package jobs

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// RemovalResult tracks the outcome of the removal operation
type RemovalResult struct {
	TotalFiles      int
	TotalDirs       int
	DeletedFiles    int
	DeletedDirs     int
	FailedFiles     []string
	FailedDirs      []string
	FileErrors      map[string]error
	DirErrors       map[string]error
}

// RemoveAllBestEffort attempts to remove all files and directories, continuing on errors
// It returns a RemovalResult showing what was deleted and what failed
func RemoveAllBestEffort(path string, logger *zap.SugaredLogger) (*RemovalResult, error) {
	result := &RemovalResult{
		FailedFiles: []string{},
		FailedDirs:  []string{},
		FileErrors:  make(map[string]error),
		DirErrors:   make(map[string]error),
	}

	// First pass: collect all files and directories
	var files []string
	var dirs []string

	err := filepath.Walk(path, func(itemPath string, info os.FileInfo, err error) error {
		if err != nil {
			// If we can't even access this path, record it and continue
			if info != nil && info.IsDir() {
				result.FailedDirs = append(result.FailedDirs, itemPath)
				result.DirErrors[itemPath] = err
			} else {
				result.FailedFiles = append(result.FailedFiles, itemPath)
				result.FileErrors[itemPath] = err
			}
			return nil // Continue walking
		}

		if info.IsDir() {
			dirs = append(dirs, itemPath)
			result.TotalDirs++
		} else {
			files = append(files, itemPath)
			result.TotalFiles++
		}
		return nil
	})

	if err != nil {
		return result, fmt.Errorf("failed to walk directory: %w", err)
	}

	// Second pass: delete all files
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			result.FailedFiles = append(result.FailedFiles, file)
			result.FileErrors[file] = err
			logger.Debugw("Failed to delete file", "file", file, "error", err)
		} else {
			result.DeletedFiles++
		}
	}

	// Third pass: delete directories in reverse order (deepest first)
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]
		// Check if directory is empty before trying to remove
		entries, err := os.ReadDir(dir)
		if err != nil {
			result.FailedDirs = append(result.FailedDirs, dir)
			result.DirErrors[dir] = err
			continue
		}

		// Only try to remove if directory is empty
		if len(entries) == 0 {
			if err := os.Remove(dir); err != nil {
				result.FailedDirs = append(result.FailedDirs, dir)
				result.DirErrors[dir] = err
				logger.Debugw("Failed to delete directory", "dir", dir, "error", err)
			} else {
				result.DeletedDirs++
			}
		} else {
			// Directory not empty, mark as failed
			result.FailedDirs = append(result.FailedDirs, dir)
			result.DirErrors[dir] = fmt.Errorf("directory not empty")
		}
	}

	return result, nil
}

