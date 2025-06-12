package jobs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestRemoveAllBestEffort(t *testing.T) {
	logger := zap.NewNop().Sugar()

	t.Run("empty directory", func(t *testing.T) {
		// Create temp directory
		tempDir := t.TempDir()
		
		// Remove it
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 1 {
			t.Errorf("Expected TotalDirs=1, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 0 {
			t.Errorf("Expected TotalFiles=0, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 1 {
			t.Errorf("Expected DeletedDirs=1, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 0 {
			t.Errorf("Expected DeletedFiles=0, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 0 {
			t.Errorf("Expected empty FailedFiles, got %v", result.FailedFiles)
		}
		if len(result.FailedDirs) != 0 {
			t.Errorf("Expected empty FailedDirs, got %v", result.FailedDirs)
		}
		
		// Verify directory is gone
		_, statErr := os.Stat(tempDir)
		if !os.IsNotExist(statErr) {
			t.Errorf("Expected directory to be deleted, but it still exists")
		}
	})

	t.Run("directory with files", func(t *testing.T) {
		// Create temp directory with files
		tempDir := t.TempDir()
		file1 := filepath.Join(tempDir, "file1.txt")
		file2 := filepath.Join(tempDir, "file2.txt")
		
		if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		
		// Remove it
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 1 {
			t.Errorf("Expected TotalDirs=1, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 2 {
			t.Errorf("Expected TotalFiles=2, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 1 {
			t.Errorf("Expected DeletedDirs=1, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 2 {
			t.Errorf("Expected DeletedFiles=2, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 0 {
			t.Errorf("Expected empty FailedFiles, got %v", result.FailedFiles)
		}
		if len(result.FailedDirs) != 0 {
			t.Errorf("Expected empty FailedDirs, got %v", result.FailedDirs)
		}
		
		// Verify everything is gone
		_, statErr := os.Stat(tempDir)
		if !os.IsNotExist(statErr) {
			t.Errorf("Expected directory to be deleted, but it still exists")
		}
	})

	t.Run("nested directories", func(t *testing.T) {
		// Create nested structure
		tempDir := t.TempDir()
		subDir1 := filepath.Join(tempDir, "subdir1")
		subDir2 := filepath.Join(subDir1, "subdir2")
		
		if err := os.MkdirAll(subDir2, 0755); err != nil {
			t.Fatalf("Failed to create nested directories: %v", err)
		}
		
		// Add files at different levels
		file1 := filepath.Join(tempDir, "file1.txt")
		file2 := filepath.Join(subDir1, "file2.txt")
		file3 := filepath.Join(subDir2, "file3.txt")
		
		if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		if err := os.WriteFile(file3, []byte("content3"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		
		// Remove it
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 3 { // root, subdir1, subdir2
			t.Errorf("Expected TotalDirs=3, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 3 {
			t.Errorf("Expected TotalFiles=3, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 3 {
			t.Errorf("Expected DeletedDirs=3, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 3 {
			t.Errorf("Expected DeletedFiles=3, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 0 {
			t.Errorf("Expected empty FailedFiles, got %v", result.FailedFiles)
		}
		if len(result.FailedDirs) != 0 {
			t.Errorf("Expected empty FailedDirs, got %v", result.FailedDirs)
		}
		
		// Verify everything is gone
		_, statErr := os.Stat(tempDir)
		if !os.IsNotExist(statErr) {
			t.Errorf("Expected directory to be deleted, but it still exists")
		}
	})

	t.Run("file with no write permission", func(t *testing.T) {
		// Create temp directory with a read-only file
		tempDir := t.TempDir()
		readOnlyFile := filepath.Join(tempDir, "readonly.txt")
		if err := os.WriteFile(readOnlyFile, []byte("content"), 0444); err != nil {
			t.Fatalf("Failed to create read-only file: %v", err)
		}
		
		// Make the directory read-only to prevent file deletion
		if err := os.Chmod(tempDir, 0555); err != nil {
			t.Fatalf("Failed to make directory read-only: %v", err)
		}
		
		// Attempt removal
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Function should not error, just record failures: %v", err)
		}
		
		// Restore permissions for cleanup
		if err := os.Chmod(tempDir, 0755); err != nil {
			t.Fatalf("Failed to restore permissions: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 1 {
			t.Errorf("Expected TotalDirs=1, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 1 {
			t.Errorf("Expected TotalFiles=1, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 0 { // Directory couldn't be deleted (has files)
			t.Errorf("Expected DeletedDirs=0, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 0 { // File couldn't be deleted
			t.Errorf("Expected DeletedFiles=0, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 1 {
			t.Errorf("Expected 1 failed file, got %d", len(result.FailedFiles))
		}
		if len(result.FailedDirs) != 1 {
			t.Errorf("Expected 1 failed directory, got %d", len(result.FailedDirs))
		}
		
		// Check that the failed file is the one we expect
		found := false
		for _, file := range result.FailedFiles {
			if file == readOnlyFile {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected %s in FailedFiles, but got %v", readOnlyFile, result.FailedFiles)
		}
		
		if result.FileErrors[readOnlyFile] == nil {
			t.Errorf("Expected error for %s in FileErrors", readOnlyFile)
		}
	})

	t.Run("mixed permissions - some files deletable", func(t *testing.T) {
		// Create temp directory with mixed permissions
		tempDir := t.TempDir()
		subDir := filepath.Join(tempDir, "subdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}
		
		// Create files with different permissions
		deletableFile := filepath.Join(tempDir, "deletable.txt")
		protectedFile := filepath.Join(subDir, "protected.txt")
		
		if err := os.WriteFile(deletableFile, []byte("can delete"), 0644); err != nil {
			t.Fatalf("Failed to create deletable file: %v", err)
		}
		if err := os.WriteFile(protectedFile, []byte("cannot delete"), 0644); err != nil {
			t.Fatalf("Failed to create protected file: %v", err)
		}
		
		// Make subdir read-only to prevent deletion of its contents
		if err := os.Chmod(subDir, 0555); err != nil {
			t.Fatalf("Failed to make subdirectory read-only: %v", err)
		}
		
		// Attempt removal
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		
		// Restore permissions for cleanup
		if err := os.Chmod(subDir, 0755); err != nil {
			t.Fatalf("Failed to restore permissions: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 2 {
			t.Errorf("Expected TotalDirs=2, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 2 {
			t.Errorf("Expected TotalFiles=2, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 0 { // Neither directory could be deleted
			t.Errorf("Expected DeletedDirs=0, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 1 { // Only deletable file was removed
			t.Errorf("Expected DeletedFiles=1, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 1 {
			t.Errorf("Expected 1 failed file, got %d", len(result.FailedFiles))
		}
		if len(result.FailedDirs) != 2 { // Both directories failed
			t.Errorf("Expected 2 failed directories, got %d", len(result.FailedDirs))
		}
		
		// Verify what remains
		_, statErr := os.Stat(deletableFile)
		if !os.IsNotExist(statErr) {
			t.Errorf("Expected deletable file to be gone, but it still exists")
		}
		_, statErr = os.Stat(protectedFile)
		if os.IsNotExist(statErr) {
			t.Errorf("Expected protected file to remain, but it was deleted")
		}
	})

	t.Run("non-existent path", func(t *testing.T) {
		// Try to remove non-existent path
		nonExistentPath := "/tmp/definitely-does-not-exist-12345"
		result, err := RemoveAllBestEffort(nonExistentPath, logger)
		
		// The behavior depends on the filesystem - filepath.Walk may or may not error
		// on non-existent paths. What matters is the result is valid.
		if result == nil {
			t.Fatalf("Expected non-nil result")
		}
		
		if err != nil {
			if !strings.Contains(err.Error(), "failed to walk directory") {
				t.Errorf("Expected error to contain 'failed to walk directory', got: %v", err)
			}
		} else {
			// If no error, we should have recorded the failure
			if len(result.FailedFiles) == 0 && len(result.FailedDirs) == 0 {
				t.Errorf("Expected some failures to be recorded when no error occurred")
			}
		}
		
		// Either way, nothing should have been successfully deleted
		if result.DeletedFiles != 0 {
			t.Errorf("Expected DeletedFiles=0, got %d", result.DeletedFiles)
		}
		if result.DeletedDirs != 0 {
			t.Errorf("Expected DeletedDirs=0, got %d", result.DeletedDirs)
		}
	})

	t.Run("symlinks", func(t *testing.T) {
		// Create temp directory with symlink
		tempDir := t.TempDir()
		targetFile := filepath.Join(tempDir, "target.txt")
		symlinkFile := filepath.Join(tempDir, "link.txt")
		
		if err := os.WriteFile(targetFile, []byte("target content"), 0644); err != nil {
			t.Fatalf("Failed to create target file: %v", err)
		}
		if err := os.Symlink(targetFile, symlinkFile); err != nil {
			t.Fatalf("Failed to create symlink: %v", err)
		}
		
		// Remove it
		result, err := RemoveAllBestEffort(tempDir, logger)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		
		// Check results
		if result.TotalDirs != 1 {
			t.Errorf("Expected TotalDirs=1, got %d", result.TotalDirs)
		}
		if result.TotalFiles != 2 { // Target and symlink
			t.Errorf("Expected TotalFiles=2, got %d", result.TotalFiles)
		}
		if result.DeletedDirs != 1 {
			t.Errorf("Expected DeletedDirs=1, got %d", result.DeletedDirs)
		}
		if result.DeletedFiles != 2 {
			t.Errorf("Expected DeletedFiles=2, got %d", result.DeletedFiles)
		}
		if len(result.FailedFiles) != 0 {
			t.Errorf("Expected empty FailedFiles, got %v", result.FailedFiles)
		}
		if len(result.FailedDirs) != 0 {
			t.Errorf("Expected empty FailedDirs, got %v", result.FailedDirs)
		}
		
		// Verify everything is gone
		_, statErr := os.Stat(tempDir)
		if !os.IsNotExist(statErr) {
			t.Errorf("Expected directory to be deleted, but it still exists")
		}
	})
}