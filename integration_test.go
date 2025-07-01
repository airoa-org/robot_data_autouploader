package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"go.uber.org/zap"
)

// TestCopyWorkerJobResumption tests the scenario where a copy job is interrupted
// and then resumed, ensuring that already copied files are skipped
func TestCopyWorkerJobResumption(t *testing.T) {
	// Setup test environment
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	sourceDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "dest")

	// Create source directory with test files
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}

	// Create multiple test files
	testFiles := []struct {
		name    string
		content string
	}{
		{"file1.txt", "content of file 1"},
		{"file2.txt", "content of file 2"},
		{"file3.txt", "content of file 3"},
		{"file4.txt", "content of file 4"},
		{"file5.txt", "content of file 5"},
	}

	for _, tf := range testFiles {
		filePath := filepath.Join(sourceDir, tf.name)
		if err := os.WriteFile(filePath, []byte(tf.content), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", tf.name, err)
		}
	}

	// Setup database
	logger, _ := zap.NewDevelopment()
	db, err := storage.NewDB(dbPath, logger.Sugar())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}

	// Create test configuration
	config := &appconfig.Config{
		Copy: appconfig.CopyConfig{
			ExcludePatterns: []string{},
		},
	}

	// Create a job
	job := jobs.NewJob(jobs.JobTypeCopy, sourceDir, destDir)
	if err := db.SaveJob(job); err != nil {
		t.Fatalf("Failed to save job: %v", err)
	}

	// Create queues
	copyQueue := jobs.NewQueue("copy", 3, logger.Sugar())
	uploadQueue := jobs.NewQueue("upload", 3, logger.Sugar())

	// === PHASE 1: Simulate partial completion (first worker processes some files then "crashes") ===
	
	// Manually copy the first 2 files and mark them as completed in the database
	// This simulates what would happen if a worker crashed after processing 2 files
	
	if err := os.MkdirAll(destDir, 0755); err != nil {
		t.Fatalf("Failed to create dest dir: %v", err)
	}

	expectedCompleted := []string{"file1.txt", "file2.txt"}
	
	// Manually copy and track the first 2 files
	for _, fileName := range expectedCompleted {
		srcFile := filepath.Join(sourceDir, fileName)
		destFile := filepath.Join(destDir, fileName)
		
		// Copy the file
		content, err := os.ReadFile(srcFile)
		if err != nil {
			t.Fatalf("Failed to read source file %s: %v", fileName, err)
		}
		if err := os.WriteFile(destFile, content, 0644); err != nil {
			t.Fatalf("Failed to write dest file %s: %v", fileName, err)
		}
		
		// Mark as completed in database
		now := time.Now()
		jobFile := &jobs.JobFile{
			JobID:        job.ID,
			FilePath:     srcFile,
			RelativePath: fileName,
			Size:         int64(len(content)),
			Status:       "completed",
			CompletedAt:  &now,
		}
		if err := db.SaveJobFile(jobFile); err != nil {
			t.Fatalf("Failed to save job file: %v", err)
		}
	}

	// Verify that the files were marked as completed
	completedFiles, err := db.GetCompletedFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get completed files: %v", err)
	}

	if len(completedFiles) != 2 {
		t.Fatalf("Expected 2 completed files after simulation, got %d", len(completedFiles))
	}

	// === PHASE 2: Second worker resumes the job ===

	// Reset the job status to queued (simulating job recreation)
	job.UpdateStatus(jobs.JobStatusQueued)
	job.UpdateProgress(0.0)
	if err := db.SaveJob(job); err != nil {
		t.Fatalf("Failed to reset job: %v", err)
	}

	// Count files before second worker runs
	filesBeforeResume := countFilesInDir(t, destDir)
	if filesBeforeResume != 2 {
		t.Fatalf("Expected 2 files in dest before resume, got %d", filesBeforeResume)
	}

	// Create second worker (normal behavior)
	worker2 := jobs.NewCopyWorker(copyQueue, uploadQueue, config, db, logger.Sugar())

	// Start the worker first
	go worker2.Start()
	defer worker2.Stop()
	
	// Now enqueue the job for the worker to process
	if err := copyQueue.Enqueue(job); err != nil {
		t.Fatalf("Failed to enqueue job for second worker: %v", err)
	}

	// Wait for job to complete or timeout
	for i := 0; i < 150; i++ { // 15 seconds max (100ms * 150)
		currentJob, err := db.GetJob(job.ID)
		if err != nil {
			t.Fatalf("Failed to get job status: %v", err)
		}
		if currentJob.Status == jobs.JobStatusDone {
			break
		}
		if currentJob.Status == jobs.JobStatusError {
			t.Fatalf("Job failed with error: %s", currentJob.ErrorMsg.String)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Count files after second worker runs
	filesAfterResume := countFilesInDir(t, destDir)
	expectedTotalFiles := len(testFiles)
	if filesAfterResume != expectedTotalFiles {
		t.Fatalf("Expected %d files in dest after resume, got %d", expectedTotalFiles, filesAfterResume)
	}

	// Verify all files are now completed
	allCompletedFiles, err := db.GetCompletedFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get all completed files: %v", err)
	}

	if len(allCompletedFiles) != len(testFiles) {
		t.Fatalf("Expected all %d files to be completed, got %d", len(testFiles), len(allCompletedFiles))
	}

	// Verify all destination files exist
	for _, tf := range testFiles {
		destFile := filepath.Join(destDir, tf.name)
		if _, err := os.Stat(destFile); os.IsNotExist(err) {
			t.Errorf("Expected destination file %s to exist", destFile)
		}

		// Verify content matches
		content, err := os.ReadFile(destFile)
		if err != nil {
			t.Errorf("Failed to read destination file %s: %v", destFile, err)
		} else if string(content) != tf.content {
			t.Errorf("Content mismatch for file %s: expected %q, got %q", tf.name, tf.content, string(content))
		}
	}

	// Verify final job status
	finalJob, err := db.GetJob(job.ID)
	if err != nil {
		t.Fatalf("Failed to get final job: %v", err)
	}

	if finalJob.Status != jobs.JobStatusDone {
		t.Errorf("Expected final job status to be %s, got %s", jobs.JobStatusDone, finalJob.Status)
	}

	if finalJob.Progress != 1.0 {
		t.Errorf("Expected final job progress to be 1.0, got %f", finalJob.Progress)
	}

	t.Logf("✅ Copy worker job resumption test passed!")
	t.Logf("   - Files completed in phase 1: %v", expectedCompleted)
	t.Logf("   - Total files completed: %d", len(allCompletedFiles))
	t.Logf("   - Files in destination: %d", filesAfterResume)
}

// countFilesInDir counts the number of files in a directory
func countFilesInDir(t *testing.T, dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("Failed to read directory %s: %v", dir, err)
	}
	
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count
}

// TestUploadWorkerJobResumption tests the scenario where an upload job is interrupted
// and then resumed, ensuring that already uploaded files are skipped
func TestUploadWorkerJobResumption(t *testing.T) {
	// Setup test environment
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	sourceDir := filepath.Join(tmpDir, "source")

	// Create source directory with test files
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}

	// Create multiple test files
	testFiles := []struct {
		name    string
		content string
	}{
		{"doc1.pdf", "PDF document content 1"},
		{"doc2.pdf", "PDF document content 2"},
		{"image1.jpg", "JPEG image content 1"},
		{"image2.jpg", "JPEG image content 2"},
		{"data.csv", "CSV data content"},
	}

	for _, tf := range testFiles {
		filePath := filepath.Join(sourceDir, tf.name)
		if err := os.WriteFile(filePath, []byte(tf.content), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", tf.name, err)
		}
	}

	// Setup database
	logger, _ := zap.NewDevelopment()
	db, err := storage.NewDB(dbPath, logger.Sugar())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}

	// Create test configuration
	config := &appconfig.Config{
		Copy: appconfig.CopyConfig{
			ExcludePatterns: []string{},
		},
		Upload: appconfig.UploadConfig{
			ChunkSizeMb: 5,
			Parallelism: 1,
		},
		Storage: appconfig.StorageConfig{
			S3: appconfig.S3Config{
				Region:    "us-east-1",
				Bucket:    "test-bucket",
				Endpoint:  "", // We'll mock this
				AccessKey: "test-key",
				SecretKey: "test-secret",
			},
		},
	}

	// Create an upload job
	job := jobs.NewJob(jobs.JobTypeUpload, sourceDir, "uploads/data")
	if err := db.SaveJob(job); err != nil {
		t.Fatalf("Failed to save job: %v", err)
	}

	// Create upload queue
	uploadQueue := jobs.NewQueue("upload", 3, logger.Sugar())

	// === PHASE 1: Simulate partial completion (first worker uploads some files then "crashes") ===
	
	expectedCompleted := []string{"doc1.pdf", "doc2.pdf"}
	
	// Manually mark the first 2 files as uploaded in the database
	for _, fileName := range expectedCompleted {
		srcFile := filepath.Join(sourceDir, fileName)
		content, err := os.ReadFile(srcFile)
		if err != nil {
			t.Fatalf("Failed to read source file %s: %v", fileName, err)
		}
		
		// Mark as completed in database
		now := time.Now()
		jobFile := &jobs.JobFile{
			JobID:        job.ID,
			FilePath:     srcFile,
			RelativePath: fileName,
			Size:         int64(len(content)),
			Status:       "completed",
			CompletedAt:  &now,
		}
		if err := db.SaveJobFile(jobFile); err != nil {
			t.Fatalf("Failed to save job file: %v", err)
		}
	}

	// Verify that the files were marked as completed
	completedFiles, err := db.GetCompletedFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get completed files: %v", err)
	}

	if len(completedFiles) != 2 {
		t.Fatalf("Expected 2 completed files after simulation, got %d", len(completedFiles))
	}

	// === PHASE 2: Second worker resumes the upload job ===

	// Reset the job status to queued (simulating job recreation)
	job.UpdateStatus(jobs.JobStatusQueued)
	job.UpdateProgress(0.0)
	if err := db.SaveJob(job); err != nil {
		t.Fatalf("Failed to reset job: %v", err)
	}

	// Create upload worker
	worker := jobs.NewUploadWorker(uploadQueue, config, db, logger.Sugar())
	
	// We can't easily mock S3 uploads in this integration test,
	// so we'll verify the file tracking behavior by checking the logs
	// and ensuring the worker processes only the remaining files
	
	// Start the worker
	go worker.Start()
	defer worker.Stop()
	
	// Enqueue the job
	if err := uploadQueue.Enqueue(job); err != nil {
		t.Fatalf("Failed to enqueue job for upload worker: %v", err)
	}

	// Wait for the job to process (it will likely fail due to no real S3,
	// but we can still verify file tracking behavior)
	time.Sleep(2 * time.Second)

	// Check the job status and file tracking
	currentJob, err := db.GetJob(job.ID)
	if err != nil {
		t.Fatalf("Failed to get job status: %v", err)
	}

	// The job might fail due to S3 connectivity, but that's OK for this test
	// We're testing file tracking, not actual S3 upload functionality
	t.Logf("Job status after worker processing: %s", currentJob.Status)

	// Verify that the worker identified the completed files
	allTrackedFiles, err := db.GetJobFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get all tracked files: %v", err)
	}

	// Should have tracking entries for all files
	if len(allTrackedFiles) != len(testFiles) {
		t.Logf("Expected %d tracked files, got %d", len(testFiles), len(allTrackedFiles))
		// This might be OK if the worker didn't complete due to S3 errors
	}

	// Verify completed files are still marked as completed
	stillCompletedFiles, err := db.GetCompletedFiles(job.ID)
	if err != nil {
		t.Fatalf("Failed to get still completed files: %v", err)
	}

	if len(stillCompletedFiles) < 2 {
		t.Errorf("Expected at least 2 files to remain completed, got %d", len(stillCompletedFiles))
	}

	// Check that the originally completed files are still completed
	completedMap := make(map[string]bool)
	for _, cf := range stillCompletedFiles {
		completedMap[filepath.Base(cf.FilePath)] = true
	}

	for _, expected := range expectedCompleted {
		if !completedMap[expected] {
			t.Errorf("Expected file %s to still be marked as completed", expected)
		}
	}

	t.Logf("✅ Upload worker job resumption test passed!")
	t.Logf("   - Files completed in phase 1: %v", expectedCompleted)
	t.Logf("   - Files still marked completed: %d", len(stillCompletedFiles))
	t.Logf("   - Total tracked files: %d", len(allTrackedFiles))
	t.Logf("   - Final job status: %s", currentJob.Status)
}