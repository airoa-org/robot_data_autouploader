package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"golang.org/x/sync/errgroup"
)

func TestCheckForNewJobs(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Create some jobs in the database (but not in the queues)
	uploadJob1 := jobs.NewJob(jobs.JobTypeUpload, "/test/source1", "/test/destination1")
	uploadJob2 := jobs.NewJob(jobs.JobTypeUpload, "/test/source2", "/test/destination2")
	copyJob1 := jobs.NewJob(jobs.JobTypeCopy, "/test/source3", "/test/destination3")

	// Save jobs to database
	if err := db.SaveJob(uploadJob1); err != nil {
		t.Fatalf("Failed to save upload job 1: %v", err)
	}
	if err := db.SaveJob(uploadJob2); err != nil {
		t.Fatalf("Failed to save upload job 2: %v", err)
	}
	if err := db.SaveJob(copyJob1); err != nil {
		t.Fatalf("Failed to save copy job 1: %v", err)
	}

	// Verify queues are empty initially
	if service.uploadQueue.Size() != 0 {
		t.Errorf("Expected upload queue to be empty, got %d jobs", service.uploadQueue.Size())
	}
	if service.copyQueue.Size() != 0 {
		t.Errorf("Expected copy queue to be empty, got %d jobs", service.copyQueue.Size())
	}

	// Run the job checker
	if err := service.checkForNewJobs(); err != nil {
		t.Fatalf("Failed to check for new jobs: %v", err)
	}

	// Verify jobs were added to queues
	if service.uploadQueue.Size() != 2 {
		t.Errorf("Expected 2 jobs in upload queue, got %d", service.uploadQueue.Size())
	}
	if service.copyQueue.Size() != 1 {
		t.Errorf("Expected 1 job in copy queue, got %d", service.copyQueue.Size())
	}

	// Run the job checker again - should not add duplicates
	if err := service.checkForNewJobs(); err != nil {
		t.Fatalf("Failed to check for new jobs (second time): %v", err)
	}

	// Verify no duplicates were added
	if service.uploadQueue.Size() != 2 {
		t.Errorf("Expected 2 jobs in upload queue after second check, got %d", service.uploadQueue.Size())
	}
	if service.copyQueue.Size() != 1 {
		t.Errorf("Expected 1 job in copy queue after second check, got %d", service.copyQueue.Size())
	}
}

func (s *Service) startJobCheckerForTest(eg *errgroup.Group, testCtx context.Context, interval time.Duration) {
	s.logger.Info("Starting periodic job checker for test")

	// Start job checker in a goroutine
	eg.Go(func() error {
		defer s.logger.Info("Job checker stopped")
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-testCtx.Done(): // Listen on the passed test context
				s.logger.Info("Job checker context cancelled, stopping")
				return testCtx.Err()
			case <-ticker.C:
				if err := s.checkForNewJobs(); err != nil {
					s.logger.Warnw("Failed to check for new jobs", "error", err)
				}
			}
		}
	})

	s.logger.Info("Job checker started")
}

func TestPeriodicJobChecker(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Create a cancellable context for the test and an errgroup tied to it
	testCtx, testCancel := context.WithCancel(context.Background())
	eg, _ := errgroup.WithContext(testCtx)

	// Start the periodic job checker with the errgroup and its context
	service.startJobCheckerForTest(eg, testCtx, 50*time.Millisecond)

	// Create a job in the database after the checker has started
	uploadJob := jobs.NewJob(jobs.JobTypeUpload, "/test/source", "/test/destination")
	if err := db.SaveJob(uploadJob); err != nil {
		t.Fatalf("Failed to save upload job: %v", err)
	}

	// Wait for the job checker to pick up the job
	found := false
	for i := 0; i < 40; i++ { // Wait up to 2 seconds
		time.Sleep(50 * time.Millisecond)
		if service.uploadQueue.Size() > 0 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected job to be picked up by periodic checker, but upload queue is still empty")
	}

	// Stop the test context to clean up the goroutine
	testCancel()
	err := eg.Wait()
	if err != nil && err != context.Canceled {
		t.Errorf("Expected errgroup to exit cleanly, but got: %v", err)
	}
}

func TestCheckForNewJobs_OnlyQueued(t *testing.T) {
	// Create test database
	db, _ := createTestDB(t)
	defer db.Close()

	// Create test service
	service := createTestService(t, db)
	defer service.copyQueue.Close()
	defer service.uploadQueue.Close()

	// Create jobs with different statuses
	queuedJob := jobs.NewJob(jobs.JobTypeUpload, "/test/queued", "/test/destination")
	queuedJob.UpdateStatus(jobs.JobStatusQueued)

	doneJob := jobs.NewJob(jobs.JobTypeUpload, "/test/done", "/test/destination")
	doneJob.UpdateStatus(jobs.JobStatusDone)

	errorJob := jobs.NewJob(jobs.JobTypeUpload, "/test/error", "/test/destination")
	errorJob.UpdateStatus(jobs.JobStatusError)

	// Save jobs to database
	if err := db.SaveJob(queuedJob); err != nil {
		t.Fatalf("Failed to save queued job: %v", err)
	}
	if err := db.SaveJob(doneJob); err != nil {
		t.Fatalf("Failed to save done job: %v", err)
	}
	if err := db.SaveJob(errorJob); err != nil {
		t.Fatalf("Failed to save error job: %v", err)
	}

	// Run the job checker
	if err := service.checkForNewJobs(); err != nil {
		t.Fatalf("Failed to check for new jobs: %v", err)
	}

	// Only the queued job should be added to the queue
	if service.uploadQueue.Size() != 1 {
		t.Errorf("Expected 1 job in upload queue (only queued job), got %d", service.uploadQueue.Size())
	}

	// Verify it's the correct job
	job, err := service.uploadQueue.Dequeue()
	if err != nil {
		t.Fatalf("Failed to dequeue job: %v", err)
	}
	if job.ID != queuedJob.ID {
		t.Errorf("Expected job ID %s, got %s", queuedJob.ID, job.ID)
	}
}
