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
}

// RecreateUploadJob recreates an upload job from an original job ID
// This is a shared function that can be used by both client and daemon
func RecreateUploadJob(storage JobStorage, originalJobID string) (*jobs.Job, error) {
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

	return newJob, nil
}
