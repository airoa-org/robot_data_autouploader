package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/lineage"
	"go.uber.org/zap"
)

// CopyWorker handles file copy jobs
type CopyWorker struct {
	queue         *Queue
	uploadQueue   *Queue
	config        *appconfig.Config
	logger        *zap.SugaredLogger
	persister     JobPersister
	lineageClient *lineage.Client
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

// NewCopyWorker creates a new copy worker
func NewCopyWorker(queue *Queue, uploadQueue *Queue, cfg *appconfig.Config, persister JobPersister, logger *zap.SugaredLogger, lineageClient *lineage.Client) *CopyWorker {
	ctx, cancel := context.WithCancel(context.Background())

	return &CopyWorker{
		queue:         queue,
		uploadQueue:   uploadQueue,
		config:        cfg,
		logger:        logger,
		persister:     persister,
		lineageClient: lineageClient,
		ctx:           ctx,
		cancelFunc:    cancel,
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

	// Initialize lineage run ID variable for use throughout the function
	var lineageRunID string

	runID, err := w.lineageClient.StartUSBCopySession(w.ctx, job)
	if err != nil {
		w.logger.Errorw("Failed to start lineage session for copy job",
			"jobID", job.ID,
			"error", err)
		// Continue without lineage tracking
	} else {
		lineageRunID = runID
		job.AddMetadata("lineage_run_id", lineageRunID)
		w.logger.Infow("Started lineage session for copy job",
			"jobID", job.ID,
			"lineageRunID", lineageRunID)
	}

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
		if lineageRunID != "" {
			if cancelErr := w.lineageClient.CancelUSBCopySession(w.ctx, lineageRunID, job); cancelErr != nil {
				w.logger.Errorw("Failed to cancel lineage session after copy error",
					"jobID", job.ID,
					"lineageRunID", lineageRunID,
					"error", cancelErr)
			}
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
		if lineageRunID != "" {
			if cancelErr := w.lineageClient.CancelUSBCopySession(w.ctx, lineageRunID, job); cancelErr != nil {
				w.logger.Errorw("Failed to cancel lineage session after copy error",
					"jobID", job.ID,
					"lineageRunID", lineageRunID,
					"error", cancelErr)
			}
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
		if lineageRunID != "" {
			if cancelErr := w.lineageClient.CancelUSBCopySession(w.ctx, lineageRunID, job); cancelErr != nil {
				w.logger.Errorw("Failed to cancel lineage session after copy error",
					"jobID", job.ID,
					"lineageRunID", lineageRunID,
					"error", cancelErr)
			}
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
		if lineageRunID != "" {
			if cancelErr := w.lineageClient.CancelUSBCopySession(w.ctx, lineageRunID, job); cancelErr != nil {
				w.logger.Errorw("Failed to cancel lineage session after copy error",
					"jobID", job.ID,
					"lineageRunID", lineageRunID,
					"error", cancelErr)
			}
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

	// Complete lineage session on success
	if lineageRunID != "" {
		if completeErr := w.lineageClient.CompleteUSBCopySession(w.ctx, lineageRunID, job); completeErr != nil {
			w.logger.Errorw("Failed to complete lineage session after successful copy",
				"jobID", job.ID,
				"lineageRunID", lineageRunID,
				"error", completeErr)
			// Don't fail the job, just log the error
		} else {
			w.logger.Infow("Completed lineage session for copy job",
				"jobID", job.ID,
				"lineageRunID", lineageRunID)
		}
	}

	if w.config.Storage.Local.RetentionPolicyOnCopy == "delete" {
		// Delete the original files/directory from the USB source after successful copy
		w.logger.Infow("Attempting to delete original data from USB source after successful copy",
			"jobID", job.ID,
			"usb_source_path", job.Source)

		result, err := RemoveAllBestEffort(job.Source, w.logger)
		if err != nil {
			w.logger.Errorw("Error during deletion attempt",
				"jobID", job.ID,
				"usb_source_path", job.Source,
				"error", err)
		}

		// Determine overall success/failure
		if result.TotalFiles == result.DeletedFiles && result.TotalDirs == result.DeletedDirs {
			w.logger.Infow("Successfully deleted all original data from USB source after copy",
				"jobID", job.ID,
				"usb_source_path", job.Source,
				"deletedFiles", result.DeletedFiles,
				"deletedDirs", result.DeletedDirs)
		} else {
			w.logger.Warnw("Partial deletion of original data from USB source. Some files/directories remain on the USB device.",
				"jobID", job.ID,
				"usb_source_path", job.Source,
				"deletedFiles", result.DeletedFiles,
				"totalFiles", result.TotalFiles,
				"deletedDirs", result.DeletedDirs,
				"totalDirs", result.TotalDirs,
				"failedFiles", len(result.FailedFiles),
				"failedDirs", len(result.FailedDirs))

			// Log details about what couldn't be deleted
			if len(result.FailedFiles) > 0 {
				w.logger.Debugw("Files that could not be deleted from USB",
					"jobID", job.ID,
					"files", result.FailedFiles)
			}
			if len(result.FailedDirs) > 0 {
				w.logger.Debugw("Directories that could not be deleted from USB",
					"jobID", job.ID,
					"directories", result.FailedDirs)
			}
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
	type fileInfo struct {
		path string
		size int64
	}
	var filesToCopy []fileInfo
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

		filesToCopy = append(filesToCopy, fileInfo{path: path, size: info.Size()})
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	// Update job metadata
	job.FileCount = len(filesToCopy)
	var totalSize int64
	for _, file := range filesToCopy {
		totalSize += file.size
	}
	job.TotalSize = totalSize
	job.ProcessedSize = 0                                // Reset processed size
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to save job metadata (file count, total size) to DB", "jobID", job.ID, "error", errDb)
		// Log error but continue
	}

	// Check for already completed files
	completedFiles, err := w.persister.GetCompletedFiles(job.ID)
	if err != nil {
		w.logger.Warnw("Failed to get completed files, will process all files", "jobID", job.ID, "error", err)
	} else if len(completedFiles) > 0 {
		w.logger.Infow("Found completed files from previous run", "jobID", job.ID, "count", len(completedFiles))

		// Create a map of completed files for fast lookup
		completedMap := make(map[string]bool)
		var completedSize int64
		for _, cf := range completedFiles {
			completedMap[cf.FilePath] = true
			completedSize += cf.Size
		}

		// Update processed size to reflect already completed files
		job.ProcessedSize = completedSize
		if job.TotalSize > 0 {
			job.UpdateProgress(float64(job.ProcessedSize) / float64(job.TotalSize))
		}
		if errDb := w.persister.SaveJob(job); errDb != nil {
			w.logger.Warnw("Failed to update job progress after checking completed files", "jobID", job.ID, "error", errDb)
		}

		// Filter out already completed files
		var pendingFiles []fileInfo
		for _, file := range filesToCopy {
			if !completedMap[file.path] {
				pendingFiles = append(pendingFiles, file)
			} else {
				w.logger.Debugw("Skipping already completed file", "file", file.path)
			}
		}
		filesToCopy = pendingFiles

		if len(filesToCopy) == 0 {
			w.logger.Infow("All files already completed", "jobID", job.ID)
			return nil
		}
	}

	// Initialize file tracking for pending files
	var jobFiles []*JobFile
	for _, file := range filesToCopy {
		relPath, _ := filepath.Rel(job.Source, file.path)
		jobFiles = append(jobFiles, &JobFile{
			JobID:        job.ID,
			FilePath:     file.path,
			RelativePath: relPath,
			Size:         file.size,
			Status:       "pending",
		})
	}

	if err := w.persister.SaveJobFiles(jobFiles); err != nil {
		w.logger.Warnw("Failed to initialize file tracking, continuing anyway", "jobID", job.ID, "error", err)
	}

	// Create destination directory structure and copy files
	for _, file := range filesToCopy {
		relPath, err := filepath.Rel(job.Source, file.path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}
		destPath := filepath.Join(job.Destination, relPath)

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create destination subdirectory: %w", err)
		}

		// Copy the file
		err = w.copySingleFileWithProgress(job, file.path, destPath)

		// Update file tracking status
		now := time.Now()
		jobFile := &JobFile{
			JobID:        job.ID,
			FilePath:     file.path,
			RelativePath: relPath,
			Size:         file.size,
		}

		if err != nil {
			jobFile.Status = "failed"
			jobFile.ErrorMessage = err.Error()
		} else {
			jobFile.Status = "completed"
			jobFile.CompletedAt = &now
		}

		if trackErr := w.persister.SaveJobFile(jobFile); trackErr != nil {
			w.logger.Warnw("Failed to update file tracking status",
				"jobID", job.ID,
				"file", file.path,
				"status", jobFile.Status,
				"error", trackErr)
		}

		if err != nil {
			return fmt.Errorf("failed to copy file %s: %w", file.path, err)
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

	// Verify file integrity by comparing hashes
	if err := w.verifyFileHash(sourcePath, destPath); err != nil {
		return fmt.Errorf("hash verification failed: %w", err)
	}

	return nil
}

// createUploadJob creates an upload job from a completed copy job
func (w *CopyWorker) createUploadJob(copyJob *Job) {
	// Destination for copy job becomes source for upload job
	uploadSourcePath := copyJob.Destination
	s3KeyPrefix := filepath.Base(copyJob.Destination)

	uploadJob := NewJob(JobTypeUpload, uploadSourcePath, s3KeyPrefix) // NewJob no longer takes db
	uploadJob.AddMetadata("original_copy_job_id", copyJob.ID)
	uploadJob.AddMetadata("original_source_path", copyJob.Destination) // Keep track of original USB source

	// Copy relevant metadata from copyJob if needed
	if dirName, ok := copyJob.Metadata["directory_name"]; ok {
		uploadJob.AddMetadata("directory_name", dirName)
	}
	if dirTaskID, ok := copyJob.Metadata["dir_task_id"]; ok {
		uploadJob.AddMetadata("dir_task_id", dirTaskID)
	}
	if lineageRunID, ok := copyJob.Metadata["lineage_run_id"]; ok {
		uploadJob.AddMetadata("parent_lineage_run_id", lineageRunID)
	}

	// Add file list and hashes to metadata
	srcFiles, err := ScanDirectoryForFiles(uploadSourcePath, &FileScanOptions{
		ExcludePatterns: w.config.Copy.ExcludePatterns,
		Logger:          w.logger,
	})
	if err != nil {
		w.logger.Warnw("Failed to scan directory for files", "source", uploadSourcePath, "error", err)
		srcFiles = []FileInfo{} // Use empty list if scanning fails
	}

	// Convert srcFiles to JSON string for metadata
	srcFilesJSON, err := json.Marshal(srcFiles)
	if err != nil {
		w.logger.Warnw("Failed to marshal src_files to JSON", "error", err)
		uploadJob.AddMetadata("src_files", "[]") // Use empty array string if marshaling fails
	} else {
		uploadJob.AddMetadata("src_files", string(srcFilesJSON))
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

// verifyFileHash computes and compares SHA256 hashes of source and destination files
func (w *CopyWorker) verifyFileHash(sourcePath, destPath string) error {
	sourceHash, err := w.computeFileHash(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to compute source file hash: %w", err)
	}

	destHash, err := w.computeFileHash(destPath)
	if err != nil {
		return fmt.Errorf("failed to compute destination file hash: %w", err)
	}

	if sourceHash != destHash {
		return fmt.Errorf("hash mismatch: source=%s, dest=%s", sourceHash, destHash)
	}

	return nil
}

// computeFileHash computes the SHA256 hash of a file
func (w *CopyWorker) computeFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to compute hash: %w", err)
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
