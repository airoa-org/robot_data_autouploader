package handlers

import (
	"fmt"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobops"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
)

// RecreateJob recreates a job from the original job ID and ensures it will be picked up for processing
// This handles creating a properly formatted job that the daemon will recognize and process
func RecreateJob(db *storage.DB, originalJobID string) (*jobs.Job, error) {
	// Use the shared job recreation logic
	newJob, err := jobops.RecreateUploadJob(db, originalJobID)
	if err != nil {
		return nil, err
	}

	// Add client-specific metadata
	newJob.AddMetadata("manually_recreated_at", time.Now().Format(time.RFC3339))
	newJob.AddMetadata("recreated_by", "client")

	// Save the updated job back to the database
	if err := db.SaveJob(newJob); err != nil {
		return nil, fmt.Errorf("failed to update recreated job: %w", err)
	}

	// The daemon has a periodic job checker that will pick up this job
	// from the database and add it to its queue automatically

	return newJob, nil
}