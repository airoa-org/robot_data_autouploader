package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage" // Added for DB access
	"go.uber.org/zap"
)

// USBStatus represents the status of a USB device
type USBStatus string

const (
	// USBStatusDetected indicates the USB device was detected
	USBStatusDetected USBStatus = "detected"

	// USBStatusProcessing indicates the USB device is being processed
	USBStatusProcessing USBStatus = "processing"

	// USBStatusCompleted indicates the USB device processing is complete
	USBStatusCompleted USBStatus = "completed"

	// USBStatusError indicates an error occurred while processing the USB device
	USBStatusError USBStatus = "error"
)

// USBDetector defines the interface for USB device detection and monitoring.
type USBDetector interface {
	Start(ctx context.Context)
	Stop()
	GetStatusChannel() <-chan *USBDeviceInfo
	GetDevices() []*USBDeviceInfo
	UpdateDeviceStatus(deviceID string, jobID string, status USBStatus, errorMsg string)
	// Add any other methods that are part of the public API of USBMonitor
	// and should be mockable, e.g., processVolume if it were public and called externally.
}

// USBDeviceInfo represents information about a USB device
type USBDeviceInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	MountPoint  string    `json:"mount_point"`
	Status      USBStatus `json:"status"`
	JobID       string    `json:"job_id,omitempty"`
	DetectedAt  time.Time `json:"detected_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	ErrorMsg    string    `json:"error_message,omitempty"`
}

// USBMonitor monitors USB devices and creates copy jobs
type USBMonitor struct {
	config    *appconfig.Config
	copyQueue *jobs.Queue
	logger    *zap.SugaredLogger
	db        *storage.DB // Added DB field

	// For tracking USB devices and directory tasks
	devices     map[string]*USBDeviceInfo
	deviceMutex sync.Mutex

	// For stopping the monitor
	stopCh chan struct{}

	// For notifying status changes
	statusCh chan *USBDeviceInfo
}

// NewUSBMonitor creates a new USB monitor
func NewUSBMonitor(config *appconfig.Config, copyQueue *jobs.Queue, db *storage.DB, logger *zap.SugaredLogger) *USBMonitor { // Added db parameter
	return &USBMonitor{
		config:    config,
		copyQueue: copyQueue,
		db:        db, // Initialize db field
		logger:    logger,
		devices:   make(map[string]*USBDeviceInfo),
		stopCh:    make(chan struct{}),
		statusCh:  make(chan *USBDeviceInfo, 10),
	}
}

// GetStatusChannel returns the status channel
func (m *USBMonitor) GetStatusChannel() <-chan *USBDeviceInfo {
	return m.statusCh
}

// GetDevices returns a copy of all tracked USB devices
func (m *USBMonitor) GetDevices() []*USBDeviceInfo {
	m.deviceMutex.Lock()
	defer m.deviceMutex.Unlock()

	devices := make([]*USBDeviceInfo, 0, len(m.devices))
	for _, device := range m.devices {
		// Create a copy of the device info
		deviceCopy := *device
		devices = append(devices, &deviceCopy)
	}

	return devices
}

// Start starts the USB monitor
func (m *USBMonitor) Start(ctx context.Context) {
	m.logger.Info("Starting USB monitor")

	// Create ticker for polling
	ticker := time.NewTicker(time.Duration(m.config.USB.ScanIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("USB monitor stopped (context canceled)")
			return
		case <-m.stopCh:
			m.logger.Info("USB monitor stopped")
			return
		case <-ticker.C:
			// Scan for USB devices
			m.checkMountedVolumes()
		}
	}
}

// Stop stops the USB monitor
func (m *USBMonitor) Stop() {
	close(m.stopCh)
}

// checkMountedVolumes checks for mounted volumes and processes their top-level directories
func (m *USBMonitor) checkMountedVolumes() {
	var volumesDir string
	if m.config.USB.TargetMountPoint != "" {
		volumesDir = m.config.USB.TargetMountPoint
	} else {
		// Platform-specific logic (simplified for brevity, consider a library for robustness)
		if _, err := os.Stat("/Volumes"); err == nil { // macOS
			volumesDir = "/Volumes"
		} else if _, err := os.Stat("/media"); err == nil { // Common Linux
			volumesDir = "/media"
		} else if _, err := os.Stat("/run/media"); err == nil { // Newer Linux
			volumesDir = "/run/media"
		} else {
			m.logger.Debug("No standard mount volume directory found (e.g., /Volumes, /media, /run/media). Specify TargetMountPoint in config if needed.")
			return
		}
	}

	volumeEntries, err := os.ReadDir(volumesDir)
	if err != nil {
		m.logger.Warnw("Error reading volumes directory", "directory", volumesDir, "error", err)
		return
	}

	for _, volEntry := range volumeEntries {
		if !volEntry.IsDir() {
			continue
		}

		volumePath := filepath.Join(volumesDir, volEntry.Name())

		// Skip ignored patterns for volume names
		isIgnored := false
		for _, pattern := range m.config.USB.IgnoredPatterns {
			matched, _ := filepath.Match(pattern, volEntry.Name())
			if matched {
				m.logger.Debugw("Skipping ignored volume", "volume", volEntry.Name(), "pattern", pattern)
				isIgnored = true
				break
			}
		}
		if isIgnored {
			continue
		}

		m.logger.Debugw("Checking volume", "path", volumePath)
		dataRoot := filepath.Join(filepath.Join(volumePath, m.config.USB.TargetDirectory))

		dirEntries, err := os.ReadDir(dataRoot)
		if err != nil {
			m.logger.Warnw("Error reading data root directory", "directory", dataRoot, "error", err)
			continue
		}

		for _, dirEntry := range dirEntries {
			if !dirEntry.IsDir() {
				continue
			}

			dirPath := filepath.Join(dataRoot, dirEntry.Name())
			dirTaskID := "dir:" + dirPath // Unique ID for this directory processing task

			m.deviceMutex.Lock()
			if devInfo, exists := m.devices[dirTaskID]; exists && devInfo.Status != USBStatusError && devInfo.Status != USBStatusDetected {
				// If status is processing, completed, etc. (but not error or just detected), skip.
				// This allows reprocessing on error or if it was only detected but not processed.
				m.deviceMutex.Unlock()
				m.logger.Debugw("Directory task already known and not in a re-processable state", "dirTaskID", dirTaskID, "status", devInfo.Status)
				continue
			}
			m.deviceMutex.Unlock()

			// Check DB if this directory has been successfully copied before
			copied, err := m.isDirCopied(dirPath)
			if err != nil {
				m.logger.Errorw("Failed to check DB for directory copy status", "dirPath", dirPath, "error", err)
				continue // Skip this directory if DB check fails
			}
			if copied {
				m.logger.Infow("Directory already copied successfully (found in DB)", "dirPath", dirPath)
				// Update internal status to completed to prevent re-processing in this session
				m.deviceMutex.Lock()
				if devInfo, exists := m.devices[dirTaskID]; exists {
					devInfo.Status = USBStatusCompleted
					devInfo.CompletedAt = time.Now()
				} else {
					m.devices[dirTaskID] = &USBDeviceInfo{
						ID:          dirTaskID,
						Name:        dirEntry.Name(),
						MountPoint:  dirPath, // Source for copy job
						Status:      USBStatusCompleted,
						DetectedAt:  time.Now(), // Or fetch from DB if available
						CompletedAt: time.Now(),
					}
				}
				m.deviceMutex.Unlock()
				continue
			}

			m.logger.Infow("New or previously failed/unprocessed directory detected for processing", "dirPath", dirPath)
			deviceInfo := &USBDeviceInfo{
				ID:         dirTaskID,
				Name:       dirEntry.Name(), // This is the directory name, e.g., "dataset1"
				MountPoint: dirPath,         // Full path to the directory on USB
				Status:     USBStatusDetected,
				DetectedAt: time.Now(),
			}

			m.deviceMutex.Lock()
			m.devices[dirTaskID] = deviceInfo
			m.deviceMutex.Unlock()

			select {
			case m.statusCh <- deviceInfo:
			default:
				m.logger.Warnw("Status channel full, skipping directory detection notification", "dirTaskID", dirTaskID)
			}
			m.processDirectoryTask(deviceInfo)
		}
	}
}

// isDirCopied checks the database to see if a directory has already been successfully copied.
func (m *USBMonitor) isDirCopied(dirPath string) (bool, error) {
	if m.db == nil {
		m.logger.Error("DB instance is nil in USBMonitor, cannot check if directory is copied")
		return false, fmt.Errorf("database connection not available")
	}

	// Use the new HasSuccessfullyCopiedJob method from storage.DB
	copied, err := m.db.HasSuccessfullyCopiedJob(dirPath)
	if err != nil {
		// The error from HasSuccessfullyCopiedJob is already logged and wrapped
		return false, err
	}
	return copied, nil
}

// processDirectoryTask processes a specific directory identified for copying.
func (m *USBMonitor) processDirectoryTask(deviceInfo *USBDeviceInfo) {
	m.logger.Infow("Processing directory task", "path", deviceInfo.MountPoint, "id", deviceInfo.ID)

	m.deviceMutex.Lock()
	if dev, exists := m.devices[deviceInfo.ID]; exists {
		dev.Status = USBStatusProcessing
		deviceInfo = dev // Use the instance from the map
		select {
		case m.statusCh <- deviceInfo:
		default:
			m.logger.Warnw("Status channel full, skipping directory processing notification", "dirTaskID", deviceInfo.ID)
		}
	} else {
		// Should not happen if called after adding to map
		m.logger.Errorw("Device info not found in map during processDirectoryTask", "id", deviceInfo.ID)
		m.deviceMutex.Unlock()
		return
	}
	m.deviceMutex.Unlock()

	// The source for the copy job is the MountPoint of the deviceInfo, which is the directory path.
	m.createCopyJobForDirectory(deviceInfo.MountPoint, deviceInfo.ID)
}

// createCopyJobForDirectory creates a copy job for the specified source directory.
func (m *USBMonitor) createCopyJobForDirectory(sourceDirPath, dirTaskID string) {
	dirName := filepath.Base(sourceDirPath)
	destPath := filepath.Join(m.config.Storage.Local.StagingDir, dirName)

	// Call NewJob with its current signature (no db argument)
	job := jobs.NewJob(jobs.JobTypeCopy, sourceDirPath, destPath)
	job.AddMetadata("directory_name", dirName)
	job.AddMetadata("dir_task_id", dirTaskID)

	// Retrieve DetectedAt from the stored deviceInfo to add to metadata
	m.deviceMutex.Lock()
	deviceInfo, exists := m.devices[dirTaskID]
	if exists {
		job.AddMetadata("detected_at", deviceInfo.DetectedAt.Format(time.RFC3339))
	} else {
		job.AddMetadata("detected_at", time.Now().Format(time.RFC3339))
		m.logger.Warnw("Device info not found in map when creating job metadata for detected_at", "dirTaskID", dirTaskID)
	}
	m.deviceMutex.Unlock()

	// Persist the newly created job to the database using SaveJob
	if err := m.db.SaveJob(job); err != nil {
		m.logger.Errorw("Failed to save copy job to database",
			"jobID", job.ID,
			"source", sourceDirPath,
			"error", err)
		m.UpdateDeviceStatus(dirTaskID, job.ID, USBStatusError, fmt.Sprintf("failed to save job to DB: %v", err))
		return // Do not enqueue if saving to DB failed
	}
	m.logger.Infow("Copy job created and saved to database", "jobID", job.ID, "source", sourceDirPath)

	if err := m.copyQueue.Enqueue(job); err != nil {
		m.logger.Errorw("Failed to enqueue copy job for directory",
			"source", sourceDirPath,
			"destination", destPath,
			"error", err)
		m.UpdateDeviceStatus(dirTaskID, "", USBStatusError, err.Error())
		return
	}

	m.logger.Infow("Copy job created for directory",
		"jobID", job.ID,
		"source", sourceDirPath,
		"destination", destPath)

	m.deviceMutex.Lock()
	if device, exists := m.devices[dirTaskID]; exists {
		device.JobID = job.ID
		// Status is already USBStatusProcessing, job creation itself doesn't change it to completed.
		// The CopyWorker will eventually update the job status, which can then update the device status.
		select {
		case m.statusCh <- device: // Notify that a job ID is now associated
		default:
			m.logger.Warnw("Status channel full, skipping job ID association notification", "dirTaskID", dirTaskID)
		}
	}
	m.deviceMutex.Unlock()
}

// UpdateDeviceStatus updates the status of a USB device or directory task
func (m *USBMonitor) UpdateDeviceStatus(deviceID string, jobID string, status USBStatus, errorMsg string) {
	m.deviceMutex.Lock()
	defer m.deviceMutex.Unlock()

	targetDeviceID := deviceID
	// Find device by job ID if device ID is not provided or if it's a job-specific update
	if jobID != "" {
		foundByJob := false
		for id, device := range m.devices {
			// dirTaskID is stored in job metadata as "dir_task_id"
			// The job's direct link to a device is via its JobID field in USBDeviceInfo
			if device.JobID == jobID {
				targetDeviceID = id
				foundByJob = true
				break
			}
		}
		if !foundByJob && deviceID == "" { // If deviceID was empty and not found by jobID
			m.logger.Warnw("UpdateDeviceStatus called with jobID but no matching device found", "jobID", jobID)
			return
		}
		// If deviceID was provided, it takes precedence unless it's empty.
		// If both are provided, we assume deviceID is the primary key.
	}

	if targetDeviceID == "" {
		m.logger.Warnw("UpdateDeviceStatus called with empty deviceID and no matching jobID found")
		return
	}

	if device, exists := m.devices[targetDeviceID]; exists {
		originalStatus := device.Status
		device.Status = status
		device.ErrorMsg = "" // Clear previous error message

		if status == USBStatusCompleted {
			device.CompletedAt = time.Now()
		}

		if status == USBStatusError && errorMsg != "" {
			device.ErrorMsg = errorMsg
		}

		// Only notify if there's a meaningful change (e.g. status change, or error message update)
		if originalStatus != device.Status || (status == USBStatusError && errorMsg != "") {
			deviceCopy := *device // Send a copy to the channel
			select {
			case m.statusCh <- &deviceCopy:
			default:
				m.logger.Warnw("Status channel full, skipping device status update notification", "deviceID", targetDeviceID)
			}
		}
		m.logger.Infow("Device status updated", "deviceID", targetDeviceID, "newStatus", status, "jobID", jobID, "error", errorMsg)
	} else {
		m.logger.Warnw("Device not found for status update", "deviceID", targetDeviceID, "jobID", jobID)
	}
}
