package jobops

import (
	"fmt"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
)

// JobGetter defines an interface for getting jobs from storage
type JobGetter interface {
	GetJob(id string) (*jobs.Job, error)
}

// JobSaver defines an interface for saving jobs to storage
type JobSaver interface {
	SaveJob(job *jobs.Job) error
}

// JobStorage combines JobGetter and JobSaver interfaces
type JobStorage interface {
	JobGetter
	JobSaver
	ResetJobFiles(jobID string) error
}

// RecreateUploadJobOptions holds options for job recreation
type RecreateUploadJobOptions struct {
	ResetFileTracking bool // If true, clear all file tracking data; if false, preserve existing progress
}

// RecreateUploadJob recreates an upload job from an original job ID
// This is a shared function that can be used by both client and daemon
func RecreateUploadJob(storage JobStorage, originalJobID string) (*jobs.Job, error) {
	return RecreateUploadJobWithOptions(storage, originalJobID, RecreateUploadJobOptions{ResetFileTracking: true})
}

// RecreateUploadJobWithOptions recreates an upload job with specific options
func RecreateUploadJobWithOptions(storage JobStorage, originalJobID string, options RecreateUploadJobOptions) (*jobs.Job, error) {
	// Get the original job
	originalJob, err := storage.GetJob(originalJobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get original job: %w", err)
	}

	// Validate that this is an upload job
	if originalJob.Type != jobs.JobTypeUpload {
		return nil, fmt.Errorf("can only recreate upload jobs, but job %s is of type %s", originalJobID, originalJob.Type)
	}

	// Create a new job with the same source and destination
	newJob := jobs.NewJob(jobs.JobTypeUpload, originalJob.Source, originalJob.Destination)

	// Copy relevant metadata from the original job
	for k, v := range originalJob.Metadata {
		newJob.AddMetadata(k, v)
	}

	// Add reference to the original job
	newJob.AddMetadata("recreated_from", originalJobID)
	newJob.AddMetadata("recreated_at", time.Now().Format(time.RFC3339))

	// Ensure job status is set to queued
	newJob.UpdateStatus(jobs.JobStatusQueued)
	newJob.UpdateProgress(0.0)

	// Save the new job to the database
	if err := storage.SaveJob(newJob); err != nil {
		return nil, fmt.Errorf("failed to save recreated job: %w", err)
	}

	// Handle file tracking based on options
	if options.ResetFileTracking {
		// Reset file tracking for the new job to start fresh
		if err := storage.ResetJobFiles(newJob.ID); err != nil {
			// Don't fail the recreation if file tracking reset fails, just log
			// The job will work without file tracking
			return newJob, nil
		}
		newJob.AddMetadata("file_tracking_reset", "true")
	} else {
		// Preserve existing file tracking data by not resetting it
		newJob.AddMetadata("file_tracking_preserved", "true")
	}

	return newJob, nil
}
