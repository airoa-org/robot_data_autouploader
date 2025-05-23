package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobops"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"go.uber.org/zap"
)

// Service represents the daemon service
type Service struct {
	config       *appconfig.Config
	logger       *zap.SugaredLogger
	db           *storage.DB
	copyQueue    *jobs.Queue
	uploadQueue  *jobs.Queue
	copyWorker   *jobs.CopyWorker
	uploadWorker *jobs.UploadWorker
	usbMonitor   USBDetector

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewService creates a new daemon service
func NewService(configPath string) (*Service, error) {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize logger
	logger, err := initLogger()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Load configuration
	cfg, err := appconfig.LoadConfig(configPath)
	if err != nil {
		cancel()
		logger.Errorw("Failed to load configuration", "error", err)
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	logger.Infow("Configuration loaded", "config_path", configPath)

	// Create service
	svc := &Service{
		config:     cfg,
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	return svc, nil
}

// Start starts the daemon service
func (s *Service) Start() error {
	s.logger.Info("Starting daemon service")

	// Initialize database
	if err := s.initDatabase(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize job queues
	if err := s.initJobQueues(); err != nil {
		return fmt.Errorf("failed to initialize job queues: %w", err)
	}

	// Initialize workers
	if err := s.initWorkers(); err != nil {
		return fmt.Errorf("failed to initialize workers: %w", err)
	}

	// Initialize USB monitor
	if err := s.initUSBMonitor(); err != nil {
		return fmt.Errorf("failed to initialize USB monitor: %w", err)
	}

	// Start workers
	s.startWorkers()

	// Start USB monitor
	s.startUSBMonitor()

	// Start periodic job checker
	s.startJobChecker()

	s.logger.Info("Daemon service started")

	return nil
}

// Stop stops the daemon service
func (s *Service) Stop() {
	s.logger.Info("Stopping daemon service")

	// Cancel context to signal all components to stop
	s.cancelFunc()

	// Stop USB monitor
	if s.usbMonitor != nil {
		s.usbMonitor.Stop()
	}

	// Stop workers
	if s.uploadWorker != nil {
		s.uploadWorker.Stop()
	}

	if s.copyWorker != nil {
		s.copyWorker.Stop()
	}

	// Close queues
	if s.uploadQueue != nil {
		s.uploadQueue.Close()
	}

	if s.copyQueue != nil {
		s.copyQueue.Close()
	}

	// Close database
	if s.db != nil {
		s.db.Close()
	}

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.logger.Info("Daemon service stopped")
}

// initLogger initializes the logger
func initLogger() (*zap.SugaredLogger, error) {
	// Create production logger
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	return logger.Sugar(), nil
}

// initDatabase initializes the database
func (s *Service) initDatabase() error {
	s.logger.Infow("Initializing database", "path", s.config.Daemon.DatabasePath)

	// Create database directory if it doesn't exist
	dbDir := filepath.Dir(s.config.Daemon.DatabasePath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize database
	db, err := storage.NewDB(s.config.Daemon.DatabasePath, s.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	s.db = db

	// Migrate database schema
	if err := s.db.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	s.logger.Info("Database initialized")

	return nil
}

// initJobQueues initializes the job queues
func (s *Service) initJobQueues() error {
	s.logger.Info("Initializing job queues")

	// Create copy queue
	s.copyQueue = jobs.NewQueue("copy", s.config.Jobs.MaxRetries, s.logger)

	// Create upload queue
	s.uploadQueue = jobs.NewQueue("upload", s.config.Jobs.MaxRetries, s.logger)

	// Load pending jobs from database
	if err := s.loadPendingJobs(); err != nil {
		return fmt.Errorf("failed to load pending jobs: %w", err)
	}

	s.logger.Info("Job queues initialized")

	return nil
}

// loadPendingJobs loads pending jobs from the database
func (s *Service) loadPendingJobs() error {
	s.logger.Info("Loading pending jobs from database")

	// First cleanup any interrupted jobs that might have been left in an inconsistent state
	if err := s.cleanupInterruptedJobs(); err != nil {
		s.logger.Warnw("Failed to cleanup interrupted jobs", "error", err)
		// Continue with loading pending jobs despite the error
	}

	// Load pending copy jobs
	copyJobs, err := s.db.GetPendingJobs(jobs.JobTypeCopy)
	if err != nil {
		return fmt.Errorf("failed to load pending copy jobs: %w", err)
	}

	// Enqueue copy jobs
	for _, job := range copyJobs {
		if err := s.copyQueue.Enqueue(job); err != nil {
			s.logger.Warnw("Failed to enqueue copy job",
				"jobID", job.ID,
				"error", err)
		}
	}

	// Load pending upload jobs
	uploadJobs, err := s.db.GetPendingJobs(jobs.JobTypeUpload)
	if err != nil {
		return fmt.Errorf("failed to load pending upload jobs: %w", err)
	}

	// Enqueue upload jobs
	for _, job := range uploadJobs {
		if err := s.uploadQueue.Enqueue(job); err != nil {
			s.logger.Warnw("Failed to enqueue upload job",
				"jobID", job.ID,
				"error", err)
		}
	}

	s.logger.Infow("Pending jobs loaded",
		"copyJobs", len(copyJobs),
		"uploadJobs", len(uploadJobs))

	return nil
}

// initWorkers initializes the workers
func (s *Service) initWorkers() error {
	s.logger.Info("Initializing workers")

	// Explicitly assert s.db to jobs.JobPersister
	var persister jobs.JobPersister
	persister = s.db // This line will fail if s.db doesn't satisfy the interface

	// Create copy worker
	s.copyWorker = jobs.NewCopyWorker(
		s.copyQueue,
		s.uploadQueue,
		s.config,
		persister, // Pass the asserted persister
		s.logger,
	)

	// Create upload worker
	s.uploadWorker = jobs.NewUploadWorker(
		s.uploadQueue,
		s.config,
		persister, // Pass the asserted persister
		s.logger,
	)

	s.logger.Info("Workers initialized")

	return nil
}

// startWorkers starts the workers
func (s *Service) startWorkers() {
	s.logger.Info("Starting workers")

	// Start copy worker
	s.copyWorker.Start()

	// Start upload worker
	s.uploadWorker.Start()

	s.logger.Info("Workers started")
}

// initUSBMonitor initializes the USB monitor
func (s *Service) initUSBMonitor() error {
	s.logger.Info("Initializing USB monitor")

	s.usbMonitor = NewUSBMonitor(s.config, s.copyQueue, s.db, s.logger)

	s.logger.Info("USB monitor initialized")

	return nil
}

// startUSBMonitor starts the USB monitor
func (s *Service) startUSBMonitor() {
	s.logger.Info("Starting USB monitor")

	// Start USB monitor
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.usbMonitor.Start(s.ctx)
	}()

	s.logger.Info("USB monitor started")
}

// WaitForShutdown waits for the service to shut down
func (s *Service) WaitForShutdown() {
	s.wg.Wait()
}

// cleanupInterruptedJobs finds any jobs in in_progress, paused, or queued states
// and updates them to error status. This is called during startup to handle
// jobs that were interrupted by a previous shutdown or crash.
func (s *Service) cleanupInterruptedJobs() error {
	s.logger.Info("Checking for interrupted jobs that need cleanup")

	// Get all jobs that might have been interrupted (in_progress, paused, queued)
	interruptedJobs, err := s.db.GetJobsByStatus(
		jobs.JobStatusInProgress,
		jobs.JobStatusPaused,
		jobs.JobStatusQueued,
	)
	if err != nil {
		return fmt.Errorf("failed to get interrupted jobs: %w", err)
	}

	if len(interruptedJobs) == 0 {
		s.logger.Info("No interrupted jobs found that need cleanup")
		return nil
	}

	s.logger.Infow("Found interrupted jobs that need cleanup", "count", len(interruptedJobs))

	// Update each job to error status
	for _, job := range interruptedJobs {
		s.logger.Infow("Cleaning up interrupted job",
			"jobID", job.ID,
			"type", job.Type,
			"status", job.Status,
			"source", job.Source,
			"destination", job.Destination)

		// Set error status
		job.SetError(fmt.Errorf("job was interrupted by system restart or shutdown"))

		// Save updated status to database
		if err := s.db.SaveJob(job); err != nil {
			s.logger.Errorw("Failed to update interrupted job status",
				"jobID", job.ID,
				"error", err)
			// Continue with other jobs despite the error
			continue
		}

		// If it's a copy job, jobs will be automatically recreated by the USB monitor
		// when it detects the same USB device and path.
		// If it's an upload job from a staging area, it will be recreated by the copy job that finishes.
	}

	s.logger.Infow("Completed cleanup of interrupted jobs", "count", len(interruptedJobs))
	return nil
}

// startJobChecker starts a goroutine that periodically checks for new jobs in the database
// and adds them to the appropriate queue
func (s *Service) startJobChecker() {
	s.logger.Info("Starting periodic job checker")

	// Start job checker in a goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Check for new jobs every 5 seconds
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				s.logger.Info("Job checker stopped")
				return
			case <-ticker.C:
				if err := s.checkForNewJobs(); err != nil {
					s.logger.Warnw("Failed to check for new jobs", "error", err)
				}
			}
		}
	}()

	s.logger.Info("Job checker started")
}

// checkForNewJobs checks for new jobs in the database and adds them to the appropriate queue
func (s *Service) checkForNewJobs() error {
	// Check for new copy jobs
	copyJobs, err := s.db.GetPendingJobs(jobs.JobTypeCopy)
	if err != nil {
		return fmt.Errorf("failed to get pending copy jobs: %w", err)
	}

	// Enqueue new copy jobs that aren't already in the queue
	for _, job := range copyJobs {
		// Check if job is already in the queue
		_, err := s.copyQueue.FindJob(job.ID)
		if err == jobs.ErrJobNotFound {
			// Job not in queue, add it
			if err := s.copyQueue.Enqueue(job); err != nil {
				s.logger.Warnw("Failed to enqueue copy job",
					"jobID", job.ID,
					"error", err)
			} else {
				s.logger.Infow("Added new copy job to queue", "jobID", job.ID)
			}
		}
	}

	// Check for new upload jobs
	uploadJobs, err := s.db.GetPendingJobs(jobs.JobTypeUpload)
	if err != nil {
		return fmt.Errorf("failed to get pending upload jobs: %w", err)
	}

	// Enqueue new upload jobs that aren't already in the queue
	for _, job := range uploadJobs {
		// Check if job is already in the queue
		_, err := s.uploadQueue.FindJob(job.ID)
		if err == jobs.ErrJobNotFound {
			// Job not in queue, add it
			if err := s.uploadQueue.Enqueue(job); err != nil {
				s.logger.Warnw("Failed to enqueue upload job",
					"jobID", job.ID,
					"error", err)
			} else {
				s.logger.Infow("Added new upload job to queue", "jobID", job.ID)
			}
		}
	}

	return nil
}

// RecreateUploadJob recreates an upload job and adds it to the upload queue
// This ensures the job is both saved to the database and added to the queue for processing
func (s *Service) RecreateUploadJob(originalJobID string) (*jobs.Job, error) {
	s.logger.Infow("Recreating upload job", "originalJobID", originalJobID)

	// Use the shared job recreation logic
	newJob, err := jobops.RecreateUploadJob(s.db, originalJobID)
	if err != nil {
		return nil, err
	}

	// Add daemon-specific metadata
	newJob.AddMetadata("manually_recreated_at", time.Now().Format(time.RFC3339))

	// Save the updated job back to the database
	if err := s.db.SaveJob(newJob); err != nil {
		return nil, fmt.Errorf("failed to update recreated job: %w", err)
	}

	// Add the job to the upload queue
	if err := s.uploadQueue.Enqueue(newJob); err != nil {
		return nil, fmt.Errorf("failed to enqueue recreated job: %w", err)
	}

	s.logger.Infow("Successfully recreated and enqueued upload job",
		"originalJobID", originalJobID,
		"newJobID", newJob.ID,
		"source", newJob.Source,
		"destination", newJob.Destination)

	return newJob, nil
}
