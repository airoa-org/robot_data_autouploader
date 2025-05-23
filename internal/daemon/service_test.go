package daemon

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"go.uber.org/zap"
)

// createTestDB creates a temporary database for testing
func createTestDB(t *testing.T) (*storage.DB, string) {
	// Create temporary directory
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create database
	db, err := storage.NewDB(dbPath, sugar)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Migrate database
	if err := db.Migrate(); err != nil {
		t.Fatalf("Failed to migrate test database: %v", err)
	}

	return db, dbPath
}

// createTestService creates a minimal service for testing
func createTestService(t *testing.T, db *storage.DB) *Service {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create service with minimal setup
	service := &Service{
		db:         db,
		logger:     sugar,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	// Create queues
	service.copyQueue = jobs.NewQueue("copy", 3, sugar)
	service.uploadQueue = jobs.NewQueue("upload", 3, sugar)

	return service
}

func TestRecreateUploadJob(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Create an original upload job
	originalJob := jobs.NewJob(jobs.JobTypeUpload, "/test/source", "/test/destination")
	originalJob.AddMetadata("test_key", "test_value")
	originalJob.UpdateStatus(jobs.JobStatusDone) // Mark as done

	// Save original job to database
	if err := db.SaveJob(originalJob); err != nil {
		t.Fatalf("Failed to save original job: %v", err)
	}

	// Recreate the job
	newJob, err := service.RecreateUploadJob(originalJob.ID)
	if err != nil {
		t.Fatalf("Failed to recreate job: %v", err)
	}

	// Verify the new job
	if newJob.ID == originalJob.ID {
		t.Errorf("New job should have different ID than original")
	}

	if newJob.Type != jobs.JobTypeUpload {
		t.Errorf("Expected job type %v, got %v", jobs.JobTypeUpload, newJob.Type)
	}

	if newJob.Source != originalJob.Source {
		t.Errorf("Expected source %v, got %v", originalJob.Source, newJob.Source)
	}

	if newJob.Destination != originalJob.Destination {
		t.Errorf("Expected destination %v, got %v", originalJob.Destination, newJob.Destination)
	}

	if newJob.Status != jobs.JobStatusQueued {
		t.Errorf("Expected status %v, got %v", jobs.JobStatusQueued, newJob.Status)
	}

	if newJob.Progress != 0.0 {
		t.Errorf("Expected progress 0.0, got %v", newJob.Progress)
	}

	// Check metadata was copied
	if newJob.Metadata["test_key"] != "test_value" {
		t.Errorf("Expected metadata test_key=test_value, got %v", newJob.Metadata["test_key"])
	}

	// Check recreation metadata was added
	if newJob.Metadata["recreated_from"] != originalJob.ID {
		t.Errorf("Expected recreated_from=%v, got %v", originalJob.ID, newJob.Metadata["recreated_from"])
	}

	if newJob.Metadata["recreated_at"] == "" {
		t.Errorf("Expected recreated_at to be set")
	}

	// Verify job was added to queue
	if service.uploadQueue.Size() != 1 {
		t.Errorf("Expected 1 job in upload queue, got %v", service.uploadQueue.Size())
	}
}

func TestRecreateUploadJob_NonUploadJob(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Create a copy job (not upload)
	copyJob := jobs.NewJob(jobs.JobTypeCopy, "/test/source", "/test/destination")

	// Save copy job to database
	if err := db.SaveJob(copyJob); err != nil {
		t.Fatalf("Failed to save copy job: %v", err)
	}

	// Try to recreate the copy job (should fail)
	_, err := service.RecreateUploadJob(copyJob.ID)
	if err == nil {
		t.Errorf("Expected error when trying to recreate non-upload job")
	}
}

func TestRecreateUploadJob_JobNotFound(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Try to recreate non-existent job
	_, err := service.RecreateUploadJob("non-existent-id")
	if err == nil {
		t.Errorf("Expected error when trying to recreate non-existent job")
	}
}
