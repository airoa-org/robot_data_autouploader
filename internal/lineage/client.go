package lineage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"go.uber.org/zap"
)

// DefaultCommandTimeout is the default timeout for lineage CLI commands
const DefaultCommandTimeout = 30 * time.Second

// Client manages interactions with airoa-lineage CLIs
type Client struct {
	config         *appconfig.Config
	logger         *zap.SugaredLogger
	commandTimeout time.Duration
}

// NewClient creates a new lineage client
// Optional timeout parameter: if not provided or <= 0, DefaultCommandTimeout (30s) is used
func NewClient(config *appconfig.Config, logger *zap.SugaredLogger, timeout ...time.Duration) *Client {
	t := DefaultCommandTimeout
	if len(timeout) > 0 && timeout[0] > 0 {
		t = timeout[0]
	}
	return &Client{
		config:         config,
		logger:         logger,
		commandTimeout: t,
	}
}

// IsEnabled checks if lineage functionality is enabled via configuration
// This is a package-level function that can be called without a client instance
func IsEnabled(config *appconfig.Config) bool {
	return config.Lineage.Enabled
}

// ValidateConfig validates required configuration when lineage is enabled
// This is a package-level function that can be called without a client instance
func ValidateConfig(config *appconfig.Config) error {
	if !config.Lineage.Enabled {
		return nil
	}

	if config.Lineage.MarquezURL == "" {
		return fmt.Errorf("MarquezURL is required when lineage.enabled=true")
	}

	return nil
}

// isEnabled checks if lineage functionality is enabled via configuration
func (c *Client) isEnabled() bool {
	return IsEnabled(c.config)
}

// validateConfig validates required configuration when lineage is enabled
func (c *Client) validateConfig() error {
	return ValidateConfig(c.config)
}

// JobProvider interface to avoid circular imports
type JobProvider interface {
	GetID() string
	GetSource() string
	GetDestination() string
	GetCreatedAt() time.Time
	GetMetadata() map[string]string
}

// StartUSBCopySession starts a USB copy lineage session with job
func (c *Client) StartUSBCopySession(ctx context.Context, job JobProvider) (string, error) {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping USB copy session start")
		return "", nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return "", err
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return "", fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildUSBCopyArgs("start", job.GetID(), metadata.RobotID, metadata.Location)

	inputDataset := c.extractUsbCopyInputDataset(job)
	args = append(args, "--input-dataset", inputDataset)

	c.logger.Debugw("Starting USB copy lineage session", "executable", c.config.Lineage.CopyExecutable, "args", args, "timeout", c.commandTimeout)
	c.logger.Debugw("Full command", "command", fmt.Sprintf("%s %s", c.config.Lineage.CopyExecutable, strings.Join(args, " ")))

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.CopyExecutable, args)
	if err != nil {
		return "", fmt.Errorf("failed to start USB copy lineage session: %w, output: %s", err, string(output))
	}

	runID := strings.TrimSpace(string(output))

	return runID, nil
}

// CompleteUSBCopySession completes a USB copy lineage session
func (c *Client) CompleteUSBCopySession(ctx context.Context, runID string, job JobProvider) error {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping USB copy session complete")
		return nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return err
	}

	if runID == "" {
		c.logger.Debug("No run ID, skipping USB copy session complete")
		return nil
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildUSBCopyArgs("complete", job.GetID(), metadata.RobotID, metadata.Location)
	args = append(args, "--run-id", runID)

	outputDataset := c.extractUsbCopyOutputDataset(job)
	args = append(args, "--output-dataset", outputDataset)

	c.logger.Debugw("Completing USB copy lineage session", "executable", c.config.Lineage.CopyExecutable, "args", args, "timeout", c.commandTimeout)

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.CopyExecutable, args)
	if err != nil {
		return fmt.Errorf("failed to complete USB copy lineage session: %w, output: %s", err, string(output))
	}

	return nil
}

// CancelUSBCopySession cancels a USB copy lineage session
func (c *Client) CancelUSBCopySession(ctx context.Context, runID string, job JobProvider) error {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping USB copy session cancel")
		return nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return err
	}

	if runID == "" {
		c.logger.Debug("No run ID, skipping USB copy session cancel")
		return nil
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildUSBCopyArgs("cancel", job.GetID(), metadata.RobotID, metadata.Location)
	args = append(args, "--run-id", runID)

	c.logger.Infow("Cancelling USB copy lineage session", "runID", runID, "timeout", c.commandTimeout)

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.CopyExecutable, args)
	if err != nil {
		return fmt.Errorf("failed to cancel USB copy lineage session: %w, output: %s", err, string(output))
	}

	c.logger.Infow("USB copy lineage session cancelled", "runID", runID)
	return nil
}

// StartS3UploadSession starts a S3 upload lineage session with job
func (c *Client) StartS3UploadSession(ctx context.Context, job JobProvider) (string, error) {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping S3 upload session start")
		return "", nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return "", err
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return "", fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildS3UploadArgs("start", job.GetID(), metadata.RobotID, metadata.Location)

	inputDataset := c.extractS3UploadInputDataset(job)
	args = append(args, "--input-dataset", inputDataset)

	c.logger.Debugw("Starting S3 upload lineage session", "executable", c.config.Lineage.UploadExecutable, "args", args, "timeout", c.commandTimeout)
	c.logger.Debugw("Full command", "command", fmt.Sprintf("%s %s", c.config.Lineage.UploadExecutable, strings.Join(args, " ")))

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.UploadExecutable, args)
	if err != nil {
		return "", fmt.Errorf("failed to start S3 upload lineage session: %w, output: %s", err, string(output))
	}

	runID := strings.TrimSpace(string(output))

	return runID, nil
}

// CompleteS3UploadSession completes a S3 upload lineage session
func (c *Client) CompleteS3UploadSession(ctx context.Context, runID string, job JobProvider) error {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping S3 upload session complete")
		return nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return err
	}

	if runID == "" {
		c.logger.Debug("No run ID, skipping S3 upload session complete")
		return nil
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildS3UploadArgs("complete", job.GetID(), metadata.RobotID, metadata.Location)
	args = append(args, "--run-id", runID)

	outputDataset := c.extractS3UploadOutputDataset(job)
	args = append(args, "--output-dataset", outputDataset)

	c.logger.Debugw("Completing S3 upload lineage session", "executable", c.config.Lineage.UploadExecutable, "args", args, "timeout", c.commandTimeout)

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.UploadExecutable, args)
	if err != nil {
		return fmt.Errorf("failed to complete S3 upload lineage session: %w, output: %s", err, string(output))
	}

	return nil
}

// CancelS3UploadSession cancels a S3 upload lineage session
func (c *Client) CancelS3UploadSession(ctx context.Context, runID string, job JobProvider) error {
	// Check if lineage is enabled
	if !c.isEnabled() {
		c.logger.Debug("Lineage is disabled, skipping S3 upload session cancel")
		return nil
	}

	// Validate configuration
	if err := c.validateConfig(); err != nil {
		c.logger.Errorw("Configuration validation failed", "error", err)
		return err
	}

	if runID == "" {
		c.logger.Debug("No run ID, skipping S3 upload session cancel")
		return nil
	}

	// Load metadata from source directory
	metadata, err := c.loadMetaData(job.GetSource())
	if err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", job.GetSource())
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildS3UploadArgs("cancel", job.GetID(), metadata.RobotID, metadata.Location)
	args = append(args, "--run-id", runID)

	c.logger.Debugw("Cancelling S3 upload lineage session", "args", args, "timeout", c.commandTimeout)

	output, err := c.execCommandWithTimeout(ctx, c.config.Lineage.UploadExecutable, args)
	if err != nil {
		return fmt.Errorf("failed to cancel S3 upload lineage session: %w, output: %s", err, string(output))
	}

	return nil
}

// getHostname returns the hostname of the current machine
func (c *Client) getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		c.logger.Warnw("Failed to get hostname, using 'unknown'", "error", err)
		return "unknown"
	}
	return hostname
}

// execCommandWithTimeout executes a command with the configured timeout
func (c *Client) execCommandWithTimeout(ctx context.Context, executable string, args []string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.commandTimeout)
	defer cancel()

	cmd := exec.CommandContext(timeoutCtx, executable, args...)
	output, err := cmd.CombinedOutput()

	if timeoutCtx.Err() == context.DeadlineExceeded {
		return output, fmt.Errorf("command timed out after %v", c.commandTimeout)
	}

	return output, err
}

// buildUSBCopyArgs builds common arguments
func (c *Client) buildUSBCopyArgs(command string, jobID string, robotID string, locationName string) []string {
	args := []string{command}

	// Add common arguments
	args = append(args, "--namespace", c.config.Lineage.Namespace)
	args = append(args, "--job-name", "usb-data-copy")
	args = append(args, "--robot-id", robotID)
	args = append(args, "--location", locationName)
	args = append(args, "--hostname", c.getHostname())
	if c.config.Lineage.MarquezURL != "" {
		args = append(args, "--marquez-url", c.config.Lineage.MarquezURL)
	}
	if c.config.Lineage.FacetPrefix != "" {
		args = append(args, "--facet-prefix", c.config.Lineage.FacetPrefix)
	}

	// Add repository information
	args = append(args, "--repository-hash", c.config.Lineage.RepositoryHash)
	args = append(args, "--repository-uri", c.config.Lineage.RepositoryURI)
	args = append(args, "--repository-tag", c.config.Lineage.RepositoryTag)
	args = append(args, "--repository-branch", c.config.Lineage.RepositoryBranch)

	// Add job ID if provided
	if jobID != "" {
		args = append(args, "--job-id", jobID)
	}

	return args
}

// buildS3UploadArgs builds common arguments
func (c *Client) buildS3UploadArgs(command string, jobID string, robotID string, locationName string) []string {
	args := []string{command}

	// Add common arguments
	args = append(args, "--namespace", c.config.Lineage.Namespace)
	args = append(args, "--job-name", "s3-data-upload")
	args = append(args, "--robot-id", robotID)
	args = append(args, "--location", locationName)
	args = append(args, "--hostname", c.getHostname())
	if c.config.Lineage.MarquezURL != "" {
		args = append(args, "--marquez-url", c.config.Lineage.MarquezURL)
	}
	if c.config.Lineage.FacetPrefix != "" {
		args = append(args, "--facet-prefix", c.config.Lineage.FacetPrefix)
	}

	// Add repository information
	args = append(args, "--repository-hash", c.config.Lineage.RepositoryHash)
	args = append(args, "--repository-uri", c.config.Lineage.RepositoryURI)
	args = append(args, "--repository-tag", c.config.Lineage.RepositoryTag)
	args = append(args, "--repository-branch", c.config.Lineage.RepositoryBranch)

	// Add job ID if provided
	if jobID != "" {
		args = append(args, "--job-id", jobID)
	}

	return args
}

// loadMetaData loads metadata from the source directory and returns the parsed MetadataInfo.
// This method does not store any state in the Client to ensure thread safety.
// Supports both v1.0 and v1.1+ metadata formats.
func (c *Client) loadMetaData(sourceDir string) (*MetadataInfo, error) {
	// Find meta.json file in the source directory
	metaPath, err := FindMetadataFile(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to find metadata file: %w", err)
	}

	// Read and parse metadata using the existing function
	metadataInfo, err := ReadMetadata(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	c.logger.Infow("Loaded metadata from meta.json",
		"robot_id", metadataInfo.RobotID,
		"location_name", metadataInfo.Location,
		"version", metadataInfo.Version,
		"source_dir", sourceDir)

	return metadataInfo, nil
}

// extractUsbCopyInputDataset extracts and reconstructs input dataset JSON from job metadata
func (c *Client) extractUsbCopyInputDataset(job JobProvider) string {
	if job == nil {
		return ""
	}

	metadata := job.GetMetadata()
	if metadata == nil {
		return ""
	}

	// Parse src_files from metadata
	var srcFiles []map[string]interface{}
	if srcFilesJSON := metadata["src_files"]; srcFilesJSON != "" {
		if err := json.Unmarshal([]byte(srcFilesJSON), &srcFiles); err != nil {
			c.logger.Warnw("Failed to parse src_files JSON from metadata", "error", err, "srcFilesJSON", srcFilesJSON)
			srcFiles = []map[string]interface{}{}
		}
	} else {
		srcFiles = []map[string]interface{}{}
	}

	// Build USB device info
	usbDevice := map[string]interface{}{}
	if fsId := metadata["fs_id"]; fsId != "" {
		usbDevice["id"] = fsId
	}
	if volumeName := metadata["volume_name"]; volumeName != "" {
		usbDevice["label"] = volumeName
	}
	if fsType := metadata["fs_type"]; fsType != "" {
		usbDevice["fs_type"] = fsType
	}

	// Reconstruct the dataset JSON from metadata
	dataset := map[string]interface{}{
		"dataset_name": job.GetSource(),
		"src_dir":      map[string]interface{}{"path": job.GetSource()},
		"usb_device":   usbDevice,
		"src_files":    srcFiles,
	}

	// Convert back to JSON string
	if jsonBytes, err := json.Marshal(dataset); err == nil {
		jsonStr := string(jsonBytes)
		return jsonStr
	} else {
		c.logger.Errorw("Failed to marshal dataset to JSON", "error", err, "dataset", dataset)
	}

	return ""
}

// extractUsbCopyOutputDataset extracts and reconstructs output dataset JSON from job metadata
func (c *Client) extractUsbCopyOutputDataset(job JobProvider) string {
	if job == nil {
		return ""
	}

	metadata := job.GetMetadata()
	if metadata == nil {
		return ""
	}

	// Reconstruct the dataset JSON from metadata
	dataset := map[string]interface{}{
		"dataset_name":    job.GetDestination(),
		"dest_dir":        map[string]interface{}{"path": job.GetDestination()},
		"operation_stats": map[string]interface{}{"duration_seconds": (time.Since(job.GetCreatedAt())).Seconds()},
	}

	// Convert back to JSON string
	if jsonBytes, err := json.Marshal(dataset); err == nil {
		jsonStr := string(jsonBytes)
		return jsonStr
	} else {
		c.logger.Errorw("Failed to marshal dataset to JSON", "error", err, "dataset", dataset)
	}

	return ""
}

// extractS3UploadInputDataset extracts and reconstructs input dataset JSON from job metadata
func (c *Client) extractS3UploadInputDataset(job JobProvider) string {
	if job == nil {
		return ""
	}

	metadata := job.GetMetadata()
	if metadata == nil {
		return ""
	}

	// Parse src_files from metadata
	var srcFiles []map[string]interface{}
	if srcFilesJSON := metadata["src_files"]; srcFilesJSON != "" {
		if err := json.Unmarshal([]byte(srcFilesJSON), &srcFiles); err != nil {
			c.logger.Warnw("Failed to parse src_files JSON from metadata", "error", err, "srcFilesJSON", srcFilesJSON)
			srcFiles = []map[string]interface{}{}
		}
	} else {
		srcFiles = []map[string]interface{}{}
	}

	// Reconstruct the dataset JSON from metadata
	dataset := map[string]interface{}{
		"dataset_name": job.GetSource(),
		"src_dir":      map[string]interface{}{"path": job.GetSource()},
		"src_files":    srcFiles,
	}

	// Convert back to JSON string
	if jsonBytes, err := json.Marshal(dataset); err == nil {
		jsonStr := string(jsonBytes)
		return jsonStr
	} else {
		c.logger.Errorw("Failed to marshal dataset to JSON", "error", err, "dataset", dataset)
	}

	return ""
}

// extractS3UploadOutputDataset extracts and reconstructs output dataset JSON from job metadata
func (c *Client) extractS3UploadOutputDataset(job JobProvider) string {
	if job == nil {
		return ""
	}

	metadata := job.GetMetadata()
	if metadata == nil {
		return ""
	}

	// Reconstruct the dataset JSON from metadata
	dataset := map[string]interface{}{
		"dataset_name":    job.GetDestination(),
		"dest_dir":        map[string]interface{}{"path": job.GetDestination()},
		"operation_stats": map[string]interface{}{"duration_seconds": (time.Since(job.GetCreatedAt())).Seconds()},
	}

	// Convert back to JSON string
	if jsonBytes, err := json.Marshal(dataset); err == nil {
		jsonStr := string(jsonBytes)
		return jsonStr
	} else {
		c.logger.Errorw("Failed to marshal dataset to JSON", "error", err, "dataset", dataset)
	}

	return ""
}
