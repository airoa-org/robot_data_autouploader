package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"go.uber.org/zap"
)

// CopyWorker handles file copy jobs
type CopyWorker struct {
	queue       *Queue
	uploadQueue *Queue
	config      *appconfig.Config
	logger      *zap.SugaredLogger
	persister   JobPersister
	ctx         context.Context
	cancelFunc  context.CancelFunc
}

// NewCopyWorker creates a new copy worker
func NewCopyWorker(queue *Queue, uploadQueue *Queue, cfg *appconfig.Config, persister JobPersister, logger *zap.SugaredLogger) *CopyWorker { // Changed db to persister
	ctx, cancel := context.WithCancel(context.Background())

	return &CopyWorker{
		queue:       queue,
		uploadQueue: uploadQueue,
		config:      cfg,
		logger:      logger,
		persister:   persister, // Initialize persister field
		ctx:         ctx,
		cancelFunc:  cancel,
	}
}

// Start starts the copy worker
func (w *CopyWorker) Start() {
	go w.processJobs()
}

// Stop stops the copy worker
func (w *CopyWorker) Stop() {
	w.cancelFunc()
}

// processJobs processes jobs from the queue
func (w *CopyWorker) processJobs() {
	w.logger.Info("Copy worker started")

	for {
		// Wait for a job to be available
		if err := w.queue.Wait(w.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				w.logger.Info("Copy worker stopped")
				return
			}
			w.logger.Warnw("Error waiting for job", "error", err)
			continue
		}

		// Dequeue a job
		job, err := w.queue.Dequeue()
		if err != nil {
			if errors.Is(err, ErrQueueClosed) {
				w.logger.Info("Copy worker stopped (queue closed)")
				return
			}
			w.logger.Warnw("Error dequeuing job", "error", err)
			continue
		}

		// Process the job
		w.processJob(job)

		// Clear the active job
		w.queue.ClearActiveJob()
	}
}

// processJob processes a single copy job
func (w *CopyWorker) processJob(job *Job) {
	w.logger.Infow("Processing copy job",
		"jobID", job.ID,
		"source", job.Source,
		"destination", job.Destination)

	// Update job status
	job.UpdateStatus(JobStatusInProgress)
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to update job status to InProgress in DB", "jobID", job.ID, "error", errDb)
		// Continue processing, but log the error
	}

	// Check if the source exists
	fileInfo, err := os.Stat(job.Source)
	if err != nil {
		w.logger.Errorw("Source not found",
			"jobID", job.ID,
			"source", job.Source,
			"error", err)
		job.SetError(fmt.Errorf("source not found: %w", err))
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Create destination directory if it doesn't exist
	destDir := job.Destination
	if !fileInfo.IsDir() {
		destDir = filepath.Dir(job.Destination)
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		w.logger.Errorw("Failed to create destination directory",
			"jobID", job.ID,
			"destination", destDir,
			"error", err)
		job.SetError(fmt.Errorf("failed to create destination directory: %w", err))
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Handle directory vs file
	if fileInfo.IsDir() {
		err = w.copyDirectory(job)
	} else {
		err = w.copySingleFileWithProgress(job, job.Source, job.Destination)
	}

	if err != nil {
		w.logger.Errorw("Copy failed",
			"jobID", job.ID,
			"error", err)
		job.SetError(err)
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Update job status
	job.UpdateStatus(JobStatusDone)
	job.UpdateProgress(1.0)
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to update job status to Done in DB", "jobID", job.ID, "error", errDb)
		// Log error but proceed with source deletion and upload job creation if applicable
	}

	w.logger.Infow("Copy job completed successfully to staging area", "jobID", job.ID, "source", job.Source, "destination", job.Destination)

	if w.config.Storage.Local.RetentionPolicy == "delete_after_upload" {
		// Delete the original files/directory from the USB source after successful copy
		w.logger.Infow("Attempting to delete original data from USB source after successful copy",
			"jobID", job.ID,
			"usb_source_path", job.Source)
		if delErr := os.RemoveAll(job.Source); delErr != nil {
			w.logger.Errorw("Failed to delete original data from USB source after copy. The data remains on the USB device.",
				"jobID", job.ID,
				"usb_source_path", job.Source,
				"error", delErr)
			// This failure does not roll back the copy. The data is safe in staging.
			// The USB device status should reflect that copy succeeded but source deletion failed.
		} else {
			w.logger.Infow("Successfully deleted original data from USB source after copy",
				"jobID", job.ID,
				"usb_source_path", job.Source)
		}
	}

	// Create upload job if not in direct upload mode
	if !w.config.Jobs.DirectUpload {
		w.createUploadJob(job)
	} else {
		w.logger.Infow("Direct upload mode is enabled. No separate upload job will be created for this copy.", "jobID", job.ID)
	}
}

// copyDirectory copies a directory recursively
func (w *CopyWorker) copyDirectory(job *Job) error {
	// Get list of files to copy
	var filesToCopy []string
	err := filepath.Walk(job.Source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check exclusion patterns
		for _, pattern := range w.config.Copy.ExcludePatterns {
			matched, err := filepath.Match(pattern, filepath.Base(path))
			if err != nil {
				return err
			}
			if matched {
				w.logger.Debugw("Skipping excluded file", "file", path, "pattern", pattern)
				return nil
			}
		}

		filesToCopy = append(filesToCopy, path)
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	// Update job metadata
	job.FileCount = len(filesToCopy)
	var totalSize int64
	for _, file := range filesToCopy {
		info, err := os.Stat(file)
		if err != nil {
			return fmt.Errorf("error getting file info for size calculation: %w", err)
		}
		totalSize += info.Size()
	}
	job.TotalSize = totalSize
	job.ProcessedSize = 0                                // Reset processed size
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to save job metadata (file count, total size) to DB", "jobID", job.ID, "error", errDb)
		// Log error but continue
	}

	// Create destination directory structure
	for _, srcPath := range filesToCopy {
		relPath, err := filepath.Rel(job.Source, srcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}
		destPath := filepath.Join(job.Destination, relPath)

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create destination subdirectory: %w", err)
		}

		if err := w.copySingleFileWithProgress(job, srcPath, destPath); err != nil {
			return fmt.Errorf("failed to copy file %s: %w", srcPath, err)
		}
		// ProcessedSize is updated within copySingleFileWithProgress and then aggregated
		// Update overall job progress after each file
		if job.TotalSize > 0 { // Avoid division by zero if directory was empty
			job.UpdateProgress(float64(job.ProcessedSize) / float64(job.TotalSize))
		}
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Warnw("Failed to save job progress to DB during directory copy", "jobID", job.ID, "error", errDb)
		}
	}
	return nil
}

// copySingleFileWithProgress copies a single file and updates job.ProcessedSize
func (w *CopyWorker) copySingleFileWithProgress(job *Job, sourcePath, destPath string) error {
	sourceFileInfo, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to get source file info: %w", err)
	}
	fileSize := sourceFileInfo.Size()

	srcFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	// Use a buffer for copying
	buf := make([]byte, 32*1024) // 32KB buffer
	var written int64

	for {
		nr, er := srcFile.Read(buf)
		if nr > 0 {
			nw, ew := destFile.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			job.ProcessedSize += int64(nw)
			written += int64(nw)
			if ew != nil {
				return fmt.Errorf("write error: %w", ew)
			}
			if nr != nw {
				return fmt.Errorf("short write")
			}
		}
		if er != nil {
			if er != io.EOF {
				return fmt.Errorf("read error: %w", er)
			}
			break // EOF
		}
	}

	if written != fileSize {
		return fmt.Errorf("copy incomplete: copied %d bytes, expected %d", written, fileSize)
	}
	return nil
}

// createUploadJob creates an upload job from a completed copy job
func (w *CopyWorker) createUploadJob(copyJob *Job) {
	// Destination for copy job becomes source for upload job
	uploadSourcePath := copyJob.Destination
	// S3 key prefix can be derived from the original source's base name or other metadata
	s3KeyPrefix := filepath.Base(copyJob.Source) // Example: use the copied directory name as prefix

	uploadJob := NewJob(JobTypeUpload, uploadSourcePath, s3KeyPrefix) // NewJob no longer takes db
	uploadJob.AddMetadata("original_copy_job_id", copyJob.ID)
	uploadJob.AddMetadata("original_source_path", copyJob.Source) // Keep track of original USB source

	// Copy relevant metadata from copyJob if needed
	if dirName, ok := copyJob.Metadata["directory_name"]; ok {
		uploadJob.AddMetadata("directory_name", dirName)
	}
	if dirTaskID, ok := copyJob.Metadata["dir_task_id"]; ok {
		uploadJob.AddMetadata("dir_task_id", dirTaskID)
	}

	// Save the new upload job to the database
	if err := w.persister.SaveJob(uploadJob); err != nil { // Use persister
		w.logger.Errorw("Failed to save upload job to database",
			"uploadJobID", uploadJob.ID,
			"copyJobID", copyJob.ID,
			"source", uploadSourcePath,
			"error", err)
		// If saving the upload job fails, we can't enqueue it.
		// The copy job is already marked as Done. This situation might need monitoring.
		return
	}
	w.logger.Infow("Upload job created and saved to database", "uploadJobID", uploadJob.ID, "source", uploadSourcePath)

	if err := w.uploadQueue.Enqueue(uploadJob); err != nil {
		w.logger.Errorw("Failed to enqueue upload job",
			"uploadJobID", uploadJob.ID,
			"copyJobID", copyJob.ID,
			"source", uploadSourcePath,
			"error", err)
		// If enqueueing fails, the job is in the DB but not in the queue.
		// This might require a recovery mechanism or manual intervention.
		// For now, just log the error. The job status in DB is still 'queued'.
		return
	}

	w.logger.Infow("Upload job enqueued",
		"uploadJobID", uploadJob.ID,
		"copyJobID", copyJob.ID,
		"source", uploadSourcePath,
		"s3_key_prefix", s3KeyPrefix)
}
