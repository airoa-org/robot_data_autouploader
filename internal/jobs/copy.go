package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

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

	// Check if there's enough disk space available for the copy operation
	hasSpace, spaceErr := w.hasEnoughDiskSpace(job.Source, job.Destination)
	if spaceErr != nil {
		w.logger.Errorw("Failed to check disk space",
			"jobID", job.ID,
			"source", job.Source,
			"destination", job.Destination,
			"error", spaceErr)
		job.SetError(fmt.Errorf("failed to check disk space: %w", spaceErr))
		if errDb := w.persister.SaveJob(job); errDb != nil {
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	if !hasSpace {
		w.logger.Errorw("Not enough disk space available for copy operation",
			"jobID", job.ID,
			"source", job.Source,
			"destination", job.Destination)

		// Get the sizes for detailed error message
		sourceSize, _ := getDirSize(job.Source)
		var stat syscall.Statfs_t
		syscall.Statfs(filepath.Dir(job.Destination), &stat)
		availableSpace := stat.Bavail * uint64(stat.Bsize)
		totalSpace := stat.Blocks * uint64(stat.Bsize)

		// Format sizes in a human-readable way
		sourceSizeGB := float64(sourceSize) / (1024 * 1024 * 1024)
		availableSpaceGB := float64(availableSpace) / (1024 * 1024 * 1024)
		totalSpaceGB := float64(totalSpace) / (1024 * 1024 * 1024)

		errorMsg := fmt.Sprintf(
			"Not enough disk space available. Required: %.2f GB, Available: %.2f GB (%.1f%% of %.2f GB). Minimum free space required: %.1f%%",
			sourceSizeGB,
			availableSpaceGB,
			(float64(availableSpace)/float64(totalSpace))*100,
			totalSpaceGB,
			w.config.Copy.MinFreeSpaceRatio*100,
		)

		job.SetError(errors.New(errorMsg))
		if errDb := w.persister.SaveJob(job); errDb != nil {
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

// getDirSize calculates the total size of a directory recursively
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// hasEnoughDiskSpace checks if there's enough disk space available for the copy operation
func (w *CopyWorker) hasEnoughDiskSpace(sourcePath string, destPath string) (bool, error) {
	// Calculate the size of the source (file or directory)
	var sourceSize int64
	var err error

	fileInfo, err := os.Stat(sourcePath)
	if err != nil {
		return false, fmt.Errorf("failed to stat source: %w", err)
	}

	if fileInfo.IsDir() {
		sourceSize, err = getDirSize(sourcePath)
		if err != nil {
			return false, fmt.Errorf("failed to calculate directory size: %w", err)
		}
	} else {
		sourceSize = fileInfo.Size()
	}

	// Get available disk space at the destination
	// Make sure the destination directory exists for checking
	destDir := destPath
	if !fileInfo.IsDir() {
		destDir = filepath.Dir(destPath)
	}

	// Create destination directory if it doesn't exist to check space
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return false, fmt.Errorf("failed to create destination directory for space check: %w", err)
	}

	// Get total and available disk space
	var stat syscall.Statfs_t
	err = syscall.Statfs(destDir, &stat)
	if err != nil {
		return false, fmt.Errorf("failed to get disk space stats: %w", err)
	}

	totalSpace := stat.Blocks * uint64(stat.Bsize)
	availableSpace := stat.Bavail * uint64(stat.Bsize)

	// Apply the minimum free space ratio from config
	minFreeSpaceRatio := w.config.Copy.MinFreeSpaceRatio
	if minFreeSpaceRatio <= 0 {
		minFreeSpaceRatio = 0.05 // Default to 5% if not specified
	}

	// Calculate required space with buffer
	requiredSpace := uint64(sourceSize)

	// Calculate free space that will remain after copy
	remainingSpace := availableSpace - requiredSpace
	remainingRatio := float64(remainingSpace) / float64(totalSpace)

	w.logger.Debugw("Disk space check",
		"sourcePath", sourcePath,
		"sourceSize", sourceSize,
		"availableSpace", availableSpace,
		"totalSpace", totalSpace,
		"requiredSpace", requiredSpace,
		"remainingSpace", remainingSpace,
		"remainingRatio", remainingRatio,
		"minFreeSpaceRatio", minFreeSpaceRatio)

	// Available space must be greater than required space AND
	// remaining ratio after copy must be greater than minimum ratio
	return availableSpace >= requiredSpace && remainingRatio >= minFreeSpaceRatio, nil
}
