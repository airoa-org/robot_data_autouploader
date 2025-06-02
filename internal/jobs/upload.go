package jobs

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config" // Added awsconfig import
	"github.com/aws/aws-sdk-go-v2/credentials"      // Added credentials import
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
)

// UploadWorker handles S3 upload jobs
type UploadWorker struct {
	queue      *Queue
	config     *appconfig.Config
	s3Client   *s3.Client
	uploader   *manager.Uploader
	logger     *zap.SugaredLogger
	persister  JobPersister // Added JobPersister field
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewUploadWorker creates a new upload worker
func NewUploadWorker(queue *Queue, cfg *appconfig.Config, persister JobPersister, logger *zap.SugaredLogger) *UploadWorker { // Added persister parameter
	ctx, cancel := context.WithCancel(context.Background())

	// Create AWS config
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Storage.S3.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Storage.S3.AccessKey,
				cfg.Storage.S3.SecretKey,
				"", // Token not needed for S3
			),
		),
	)

	if err != nil {
		logger.Errorw("Failed to create AWS config", "error", err)
		// Create a minimal worker that will fail gracefully
		return &UploadWorker{
			queue:      queue,
			config:     cfg,
			logger:     logger,
			persister:  persister, // Initialize persister
			ctx:        ctx,
			cancelFunc: cancel,
		}
	}

	// Create S3 client with custom endpoint if specified
	var s3Client *s3.Client
	if cfg.Storage.S3.Endpoint != "" {
		// Use BaseEndpoint for custom endpoints (replaces deprecated EndpointResolver)
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Storage.S3.Endpoint)
			o.UsePathStyle = true // Often needed for custom S3-compatible endpoints
		})
	} else {
		s3Client = s3.NewFromConfig(awsCfg)
	}

	// Create uploader with custom transfer manager
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = int64(cfg.Upload.ChunkSizeMb) * 1024 * 1024
		u.Concurrency = cfg.Upload.Parallelism
		u.LeavePartsOnError = false // Corresponds to Upload.AbortOnFailure
	})

	return &UploadWorker{
		queue:      queue,
		config:     cfg,
		s3Client:   s3Client,
		uploader:   uploader,
		logger:     logger,
		persister:  persister, // Initialize persister
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Start starts the upload worker
func (w *UploadWorker) Start() {
	go w.processJobs()
}

// Stop stops the upload worker
func (w *UploadWorker) Stop() {
	w.cancelFunc()
}

// processJobs processes jobs from the queue
func (w *UploadWorker) processJobs() {
	w.logger.Info("Upload worker started")

	for {
		// Wait for a job to be available
		if err := w.queue.Wait(w.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				w.logger.Info("Upload worker stopped")
				return
			}
			w.logger.Warnw("Error waiting for job", "error", err)
			continue
		}

		// Dequeue a job
		job, err := w.queue.Dequeue()
		if err != nil {
			if errors.Is(err, ErrQueueClosed) {
				w.logger.Info("Upload worker stopped (queue closed)")
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

// processJob processes a single upload job
func (w *UploadWorker) processJob(job *Job) {
	w.logger.Infow("Processing upload job",
		"jobID", job.ID,
		"source", job.Source,
		"destination", job.Destination)

	// Update job status
	job.UpdateStatus(JobStatusInProgress)
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to update job status to InProgress in DB", "jobID", job.ID, "error", errDb)
		// Continue processing, but log the error
	}

	// Check if S3 client is initialized (it might not be if AWS config failed)
	if w.s3Client == nil || w.uploader == nil {
		errMsg := "S3 client not initialized, cannot process upload job. This usually indicates an issue with AWS configuration during startup."
		w.logger.Errorw(errMsg, "jobID", job.ID)
		job.SetError(errors.New(errMsg))
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB (S3 client init failure)", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Check if the source exists
	fileInfo, err := os.Stat(job.Source)
	if err != nil {
		w.logger.Errorw("Source file not found",
			"jobID", job.ID,
			"source", job.Source,
			"error", err)
		job.SetError(fmt.Errorf("source file not found: %w", err))
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Handle directory vs file
	if fileInfo.IsDir() {
		err = w.uploadDirectory(job)
	} else {
		err = w.uploadFile(job, job.Source, job.Destination)
	}

	if err != nil {
		w.logger.Errorw("Upload failed",
			"jobID", job.ID,
			"error", err)
		job.SetError(err)
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Errorw("Failed to save job error status to DB", "jobID", job.ID, "error", errDb)
		}
		return
	}

	// Create upload complete marker for the entire job at destination root
	w.createUploadCompleteMarker(job)

	// Update job status
	job.UpdateStatus(JobStatusDone)
	job.UpdateProgress(1.0)
	if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
		w.logger.Errorw("Failed to update job status to Done in DB", "jobID", job.ID, "error", errDb)
		// Log error but proceed with source deletion if applicable
	}

	w.logger.Infow("Upload job completed", "jobID", job.ID)

	// Delete source from staging if configured
	if w.config.Storage.Local.RetentionPolicyOnUpload == "delete" {
		// Only delete if not the source directory
		if filepath.Dir(job.Source) != job.Source {
			err = os.RemoveAll(job.Source)
			if err != nil {
				w.logger.Warnw("Failed to delete source directory",
					"jobID", job.ID,
					"source", job.Source,
					"error", err)
				// Don't fail the job for this
			}
		}
	}
}

// uploadDirectory uploads all files in a directory
func (w *UploadWorker) uploadDirectory(job *Job) error {
	// Get list of files to upload
	var filesToUpload []string
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
				return nil
			}
		}

		filesToUpload = append(filesToUpload, path)
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	// Update job metadata
	job.FileCount = len(filesToUpload)
	var totalSize int64
	for _, file := range filesToUpload {
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

	// Upload each file
	for _, filePath := range filesToUpload {
		// Calculate relative path
		relPath, err := filepath.Rel(job.Source, filePath)
		if err != nil {
			return fmt.Errorf("error calculating relative path: %w", err)
		}

		// Create destination key
		destKey := filepath.Join(job.Destination, relPath)
		destKey = filepath.ToSlash(destKey) // Convert to forward slashes for S3

		// Get file size
		info, err := os.Stat(filePath)
		if err != nil {
			return fmt.Errorf("error getting file info: %w", err)
		}

		// Upload the file
		err = w.uploadFile(job, filePath, destKey)
		if err != nil {
			return fmt.Errorf("error uploading file %s: %w", filePath, err)
		}

		// Update progress
		job.ProcessedSize += info.Size()
		job.UpdateProgress(float64(job.ProcessedSize) / float64(job.TotalSize))
		if errDb := w.persister.SaveJob(job); errDb != nil { // Use persister
			w.logger.Warnw("Failed to save job progress to DB during directory upload", "jobID", job.ID, "error", errDb)
		}
	}
	return nil
}

// calculateS3ETag calculates the ETag for a local file, compatible with S3's ETag calculation.
// For files uploaded via single PutObject, ETag is the MD5 hash of the file.
// For multipart uploads, ETag is MD5 of concatenated part MD5s, followed by "-N" (number of parts).
// This function assumes single part for simplicity if total size < chunkSizeMb * 1MB,
// otherwise it calculates multipart ETag.
func calculateS3ETag(filePath string, chunkSizeMb int) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	chunkSize := int64(chunkSizeMb) * 1024 * 1024
	if chunkSize <= 0 {
		chunkSize = 5 * 1024 * 1024 // Default to 5MB if not set
	}

	var md5s [][]byte
	buf := make([]byte, chunkSize)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			h := md5.New()
			h.Write(buf[:n])
			md5s = append(md5s, h.Sum(nil))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	
	// Handle 0-size files
	if len(md5s) == 0 {
		// For 0-size files, S3 returns the MD5 of empty content without part count suffix
		h := md5.New()
		return fmt.Sprintf("%x", h.Sum(nil)), nil
	}
	
	if len(md5s) == 1 {
		return fmt.Sprintf("%x", md5s[0]), nil
	}
	// Multipart ETag
	all := []byte{}
	for _, m := range md5s {
		all = append(all, m...)
	}
	h := md5.New()
	h.Write(all)
	final := h.Sum(nil)
	return fmt.Sprintf("%x-%d", final, len(md5s)), nil
}

// isFileAllowed checks if a file matches the allowed patterns
func (w *UploadWorker) isFileAllowed(filePath string) bool {
	// If no allowed patterns are configured, allow all files
	if len(w.config.Upload.AllowedPatterns) == 0 {
		return true
	}

	fileName := filepath.Base(filePath)

	// Check if file matches any of the allowed patterns
	for _, pattern := range w.config.Upload.AllowedPatterns {
		matched, err := filepath.Match(pattern, fileName)
		if err != nil {
			w.logger.Warnw("Invalid allowed pattern", "pattern", pattern, "error", err)
			continue
		}
		if matched {
			return true
		}
	}

	return false
}

// uploadFile uploads a single file
func (w *UploadWorker) uploadFile(job *Job, filePath, s3Key string) error {
	// Check if file is allowed by whitelist
	if !w.isFileAllowed(filePath) {
		w.logger.Infow("File not allowed by whitelist, skipping upload",
			"jobID", job.ID,
			"filePath", filePath,
			"allowedPatterns", w.config.Upload.AllowedPatterns)
		return nil
	}
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	}

	uploadPath := w.config.Storage.S3.UploadPath
	finalS3Key := s3Key
	if uploadPath != "" {
		uploadPath = filepath.ToSlash(uploadPath)
		if len(uploadPath) > 0 && uploadPath[len(uploadPath)-1] == '/' {
			uploadPath = uploadPath[:len(uploadPath)-1]
		}
		finalS3Key = uploadPath + "/" + filepath.ToSlash(s3Key)
		if len(finalS3Key) > 0 && finalS3Key[0] == '/' {
			finalS3Key = finalS3Key[1:]
		}
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(w.config.Storage.S3.Bucket),
		Key:    aws.String(finalS3Key),
	}
	headOutput, headErr := w.s3Client.HeadObject(w.ctx, headInput)

	if headErr == nil { // Object exists, check for deduplication
		var remoteETag string
		if headOutput.ETag != nil {
			remoteETag = aws.ToString(headOutput.ETag)
			if len(remoteETag) > 1 && remoteETag[0] == '"' && remoteETag[len(remoteETag)-1] == '"' {
				remoteETag = remoteETag[1 : len(remoteETag)-1]
			}
		}

		var remoteSize int64
		if headOutput.ContentLength != nil {
			remoteSize = *headOutput.ContentLength
		}

		if fileInfo.Size() == remoteSize {
			localETag, etagErr := calculateS3ETag(filePath, w.config.Upload.ChunkSizeMb)
			if etagErr != nil {
				w.logger.Warnw("Failed to calculate local ETag for deduplication, proceeding with upload",
					"jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key, "error", etagErr)
				// Proceed to upload if ETag calculation fails
			} else if localETag == remoteETag {
				w.logger.Infow("File already exists in S3 with matching size and ETag, skipping upload",
					"jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key, "size", fileInfo.Size(), "etag", localETag)

				// If this is a single-file job, update its progress to 100%.
				if job.FileCount <= 1 && job.TotalSize == fileInfo.Size() {
					job.ProcessedSize = fileInfo.Size()
					job.UpdateProgress(1.0)
				}
				if errDb := w.persister.SaveJob(job); errDb != nil {
					w.logger.Warnw("Failed to save job state to DB after deduplication skip", "jobID", job.ID, "error", errDb)
				}
				return nil
			} else {
				w.logger.Warnw("File exists in S3 with matching size but different ETag, skipping upload",
					"jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key, "localETag", localETag, "remoteETag", remoteETag)
				// Something went wrong, return error
				return fmt.Errorf("file exists in S3 with matching size but different ETag, skipping upload: %s", finalS3Key)

			}
		} else {
			w.logger.Warnw("File exists in S3 but with different size, skipping upload",
				"jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key, "localSize", fileInfo.Size(), "remoteSize", remoteSize)
			return fmt.Errorf("file exists in S3 but with different size, skipping upload: %s", finalS3Key)

		}
	} else { // Error checking S3 object, or object does not exist
		var nsk *types.NoSuchKey
		if errors.As(headErr, &nsk) {
			w.logger.Infow("File does not exist in S3, proceeding with upload", "jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key)
		} else if strings.Contains(headErr.Error(), "404") {
			w.logger.Infow("File does not exist in S3, proceeding with upload", "jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key)
		} else {
			w.logger.Warnw("Error checking S3 for file existence",
				"jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key, "error", headErr)
			return fmt.Errorf("error checking S3 for file existence: %w", headErr)
		}
	}

	// If we've reached this point, an upload is required.

	// Ensure job metadata (FileCount, TotalSize) is set if uploadFile
	// is called directly for a single file job (not through uploadDirectory).
	if job.FileCount == 0 {
		job.FileCount = 1
	}
	if job.TotalSize == 0 {
		job.TotalSize = fileInfo.Size()
	}

	// Save job metadata if it was potentially updated (for single file scenario)
	if job.FileCount == 1 && job.TotalSize == fileInfo.Size() {
		if errDb := w.persister.SaveJob(job); errDb != nil {
			w.logger.Errorw("Failed to save job metadata for single file upload to DB", "jobID", job.ID, "error", errDb)
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s for upload: %w", filePath, err)
	}
	defer file.Close()

	var reader io.Reader = file
	throttleMbps := w.config.Upload.ThrottleMbps
	if throttleMbps > 0 {
		bytesPerSecond := int64(throttleMbps) * 1024 * 1024 / 8
		if bytesPerSecond > 0 {
			w.logger.Debugw("Throttling upload enabled", "jobID", job.ID, "filePath", filePath, "mbps", throttleMbps, "bytesPerSecond", bytesPerSecond)
			reader = &throttledReader{
				reader:         file,
				bytesPerSecond: bytesPerSecond,
			}
		} else {
			w.logger.Warnw("Invalid throttle_mbps value, throttling disabled for this file", "jobID", job.ID, "filePath", filePath, "throttle_mbps", throttleMbps)
		}
	}

	baseProcessedBytesForJob := job.ProcessedSize

	progressReader := &ProgressReader{
		Reader: reader,
		Size:   fileInfo.Size(),
		OnProgress: func(fileBytesRead int64, fileTotalBytes int64) {
			currentFileProgress := 0.0
			if fileTotalBytes > 0 {
				currentFileProgress = float64(fileBytesRead) / float64(fileTotalBytes)
			}
			w.logger.Debugf("Job %s: File %s upload progress: %.2f%% (%d/%d bytes)",
				job.ID, filepath.Base(filePath), currentFileProgress*100, fileBytesRead, fileTotalBytes)

			if job.TotalSize > 0 {
				overallJobProcessedBytes := baseProcessedBytesForJob + fileBytesRead
				overallJobProgress := float64(overallJobProcessedBytes) / float64(job.TotalSize)
				job.UpdateProgress(overallJobProgress)
			}
		},
	}

	w.logger.Infow("Starting S3 upload",
		"jobID", job.ID,
		"filePath", filePath,
		"s3Key", finalS3Key,
		"size", fileInfo.Size())

	_, err = w.uploader.Upload(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.config.Storage.S3.Bucket),
		Key:    aws.String(finalS3Key),
		Body:   progressReader,
	})

	if err != nil {
		return fmt.Errorf("failed to upload file %s to S3: %w", filePath, err)
	}

	w.logger.Infow("S3 upload completed, verifying integrity", "jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key)

	// Verify upload integrity by comparing size and ETag
	if err := w.verifyUploadIntegrity(filePath, finalS3Key, fileInfo.Size()); err != nil {
		return fmt.Errorf("upload verification failed for %s: %w", filePath, err)
	}

	w.logger.Infow("S3 upload completed and verified successfully", "jobID", job.ID, "filePath", filePath, "s3Key", finalS3Key)


	if job.FileCount <= 1 && job.TotalSize == fileInfo.Size() {
		job.ProcessedSize = fileInfo.Size()
		job.UpdateProgress(1.0)
	}

	if errDb := w.persister.SaveJob(job); errDb != nil {
		w.logger.Warnw("Failed to save job state to DB after file upload", "jobID", job.ID, "error", errDb)
	}

	return nil
}

// throttledReader is an io.Reader that throttles read speed
type throttledReader struct {
	reader         io.Reader // The underlying reader (e.g., *os.File)
	bytesPerSecond int64     // Target bytes per second
	lastRead       time.Time // Timestamp of the end of the last read operation or start of throttling period
	bytesRead      int64     // Total bytes read since lastRead (or since throttling started for this instance)
}

func (r *throttledReader) Read(p []byte) (n int, err error) {
	if r.bytesPerSecond <= 0 {
		return r.reader.Read(p)
	}

	if r.lastRead.IsZero() {
		r.lastRead = time.Now()
		// r.bytesRead is already 0
	}

	// Calculate how many bytes we *should have* processed by now to maintain the average rate.
	elapsed := time.Since(r.lastRead)
	targetBytesProcessedThisWindow := int64(elapsed.Seconds() * float64(r.bytesPerSecond))

	// If r.bytesRead (total bytes actually read in this window) is ahead of targetBytesProcessedThisWindow,
	// we need to sleep until the target "catches up".
	if r.bytesRead > targetBytesProcessedThisWindow {
		bytesAhead := r.bytesRead - targetBytesProcessedThisWindow
		timeToWait := time.Duration(float64(bytesAhead)/float64(r.bytesPerSecond)*float64(time.Second)) + time.Millisecond

		if timeToWait > 0 {
			time.Sleep(timeToWait)
			// After sleeping, update elapsed time and reset the window for the next calculation cycle
			r.bytesRead = 0         // Reset bytes read for the new window
			r.lastRead = time.Now() // Reset window start time
		}
	} else if elapsed.Seconds() >= 1.0 { // If more than a second passed, reset window
		r.bytesRead = 0
		r.lastRead = time.Now()
	}

	// Determine the actual number of bytes to read in this call.
	// Limit the chunk size to avoid large bursts and to make throttling more responsive.
	// Read up to 1/10th of bytesPerSecond, or len(p), whichever is smaller.
	// Ensure a minimum practical chunk size if rate is very low.
	chunkSize := r.bytesPerSecond / 10
	if chunkSize == 0 {
		chunkSize = 128
	}
	if chunkSize <= 0 && len(p) > 0 {
		chunkSize = 1 // Ensure chunkSize is at least 1 if we can read something.
	}

	readLen := int(chunkSize)
	if readLen > len(p) {
		readLen = len(p)
	}
	if readLen <= 0 && len(p) > 0 {
		readLen = 1
	} else if len(p) == 0 {
		readLen = 0
	}

	if readLen > 0 {
		n, err = r.reader.Read(p[:readLen])
	} else {
		n, err = r.reader.Read(p) // Standard behavior for empty p or if readLen became 0
	}

	if n > 0 {
		r.bytesRead += int64(n) // Accumulate bytes read in the current window
	}

	return n, err
}

// verifyUploadIntegrity verifies that the uploaded file matches the local file by comparing size and ETag
func (w *UploadWorker) verifyUploadIntegrity(localFilePath, s3Key string, expectedSize int64) error {
	// Get remote object metadata
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(w.config.Storage.S3.Bucket),
		Key:    aws.String(s3Key),
	}

	headOutput, err := w.s3Client.HeadObject(w.ctx, headInput)
	if err != nil {
		return fmt.Errorf("failed to get remote object metadata: %w", err)
	}

	// Verify size
	var remoteSize int64
	if headOutput.ContentLength != nil {
		remoteSize = *headOutput.ContentLength
	}

	if remoteSize != expectedSize {
		return fmt.Errorf("size mismatch: local=%d, remote=%d", expectedSize, remoteSize)
	}

	// Calculate expected ETag for local file
	expectedETag, err := calculateS3ETag(localFilePath, w.config.Upload.ChunkSizeMb)
	if err != nil {
		return fmt.Errorf("failed to calculate local file ETag: %w", err)
	}

	// Get remote ETag
	var remoteETag string
	if headOutput.ETag != nil {
		remoteETag = aws.ToString(headOutput.ETag)
		// Remove quotes if present
		if len(remoteETag) > 1 && remoteETag[0] == '"' && remoteETag[len(remoteETag)-1] == '"' {
			remoteETag = remoteETag[1 : len(remoteETag)-1]
		}
	}

	// Compare ETags
	if expectedETag != remoteETag {
		return fmt.Errorf("ETag mismatch: local=%s, remote=%s", expectedETag, remoteETag)
	}

	w.logger.Debugw("Upload integrity verified",
		"localFilePath", localFilePath,
		"s3Key", s3Key,
		"size", expectedSize,
		"etag", expectedETag)

	return nil
}

// createUploadCompleteMarker creates an upload complete marker file at the job destination root
func (w *UploadWorker) createUploadCompleteMarker(job *Job) {
	// Create upload complete marker for the entire job at destination root
	uploadPath := w.config.Storage.S3.UploadPath
	markerS3Key := filepath.ToSlash(job.Destination) + "/.upload_complete"
	if uploadPath != "" {
		uploadPath = filepath.ToSlash(uploadPath)
		if len(uploadPath) > 0 && uploadPath[len(uploadPath)-1] == '/' {
			uploadPath = uploadPath[:len(uploadPath)-1]
		}
		markerS3Key = uploadPath + "/" + markerS3Key
		if len(markerS3Key) > 0 && markerS3Key[0] == '/' {
			markerS3Key = markerS3Key[1:]
		}
	}

	// Upload an empty file as the marker
	_, markerErr := w.s3Client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.config.Storage.S3.Bucket),
		Key:    aws.String(markerS3Key),
		Body:   strings.NewReader(""),
	})

	if markerErr != nil {
		w.logger.Warnw("Failed to create upload_complete marker file for job",
			"jobID", job.ID,
			"s3Key", markerS3Key,
			"error", markerErr)
		// Don't fail the job if marker creation fails, just log a warning
	} else {
		w.logger.Infow("Created upload_complete marker file for job",
			"jobID", job.ID,
			"s3Key", markerS3Key)
	}
}
