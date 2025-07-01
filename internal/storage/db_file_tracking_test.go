package storage

import (
	"path/filepath"
	"testing"
	"time"

	j "github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"go.uber.org/zap"
)

func TestFileTracking(t *testing.T) {
	// Create temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	logger, _ := zap.NewDevelopment()
	db, err := NewDB(dbPath, logger.Sugar())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Migrate database
	if err := db.Migrate(); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}

	// Create a test job
	job := j.NewJob(j.JobTypeCopy, "/source", "/dest")
	if err := db.SaveJob(job); err != nil {
		t.Fatalf("Failed to save job: %v", err)
	}

	// Test saving job files
	now := time.Now()
	jobFiles := []*j.JobFile{
		{
			JobID:        job.ID,
			FilePath:     "/source/file1.txt",
			RelativePath: "file1.txt",
			Size:         100,
			Status:       "pending",
		},
		{
			JobID:        job.ID,
			FilePath:     "/source/file2.txt",
			RelativePath: "file2.txt",
			Size:         200,
			Status:       "completed",
			CompletedAt:  &now,
			Checksum:     "abc123",
		},
	}

	if err := db.SaveJobFiles(jobFiles); err != nil {
		t.Fatalf("Failed to save job files: %v", err)
	}

	// Test getting all job files
	allFiles, err := db.GetJobFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get job files: %v", err)
	}

	if len(allFiles) != 2 {
		t.Errorf("Expected 2 files, got %d", len(allFiles))
	}

	// Test getting pending files
	pendingFiles, err := db.GetPendingFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get pending files: %v", err)
	}

	if len(pendingFiles) != 1 {
		t.Errorf("Expected 1 pending file, got %d", len(pendingFiles))
	}

	if pendingFiles[0].FilePath != "/source/file1.txt" {
		t.Errorf("Expected pending file to be file1.txt, got %s", pendingFiles[0].FilePath)
	}

	// Test getting completed files
	completedFiles, err := db.GetCompletedFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get completed files: %v", err)
	}

	if len(completedFiles) != 1 {
		t.Errorf("Expected 1 completed file, got %d", len(completedFiles))
	}

	if completedFiles[0].FilePath != "/source/file2.txt" {
		t.Errorf("Expected completed file to be file2.txt, got %s", completedFiles[0].FilePath)
	}

	// Test file tracking stats
	total, completed, failed, err := db.GetFileTrackingStats(job.ID)
	if err != nil {
		t.Fatalf("Failed to get file tracking stats: %v", err)
	}

	if total != 2 {
		t.Errorf("Expected total 2, got %d", total)
	}
	if completed != 1 {
		t.Errorf("Expected completed 1, got %d", completed)
	}
	if failed != 0 {
		t.Errorf("Expected failed 0, got %d", failed)
	}

	// Test updating file status
	fileToUpdate := &j.JobFile{
		JobID:        job.ID,
		FilePath:     "/source/file1.txt",
		RelativePath: "file1.txt",
		Size:         100,
		Status:       "completed",
		CompletedAt:  &now,
		Checksum:     "def456",
	}

	if err := db.SaveJobFile(fileToUpdate); err != nil {
		t.Fatalf("Failed to update job file: %v", err)
	}

	// Verify update
	updatedStats, _, _, err := db.GetFileTrackingStats(job.ID)
	if err != nil {
		t.Fatalf("Failed to get updated stats: %v", err)
	}

	if updatedStats != 2 {
		t.Errorf("Expected stats to show 2 completed files")
	}

	// Test resetting job files
	if err := db.ResetJobFiles(job.ID); err != nil {
		t.Fatalf("Failed to reset job files: %v", err)
	}

	// Verify reset
	resetFiles, err := db.GetJobFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get files after reset: %v", err)
	}

	if len(resetFiles) != 0 {
		t.Errorf("Expected 0 files after reset, got %d", len(resetFiles))
	}
}