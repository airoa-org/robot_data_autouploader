package lineage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"go.uber.org/zap"
)

// Client manages interactions with airoa-lineage CLIs
type Client struct {
	config       *appconfig.Config
	logger       *zap.SugaredLogger
	robotID      string
	locationName string
}

// NewClient creates a new lineage client
func NewClient(config *appconfig.Config, logger *zap.SugaredLogger) *Client {
	return &Client{
		config: config,
		logger: logger,
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

// StartUSBCopySession starts a USB copy lineage session
func (c *Client) StartUSBCopySession(ctx context.Context, sourceDir string, jobID string) (string, error) {
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
	if err := c.loadMetaData(sourceDir); err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", sourceDir)
		return "", fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildUSBCopyArgs("start", jobID)

	c.logger.Debugw("Starting USB copy lineage session", "args", args)

	cmd := exec.CommandContext(ctx, c.config.Lineage.CopyExecutable, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to start USB copy lineage session: %w, output: %s", err, string(output))
	}

	runID := strings.TrimSpace(string(output))

	return runID, nil
}

// CompleteUSBCopySession completes a USB copy lineage session
func (c *Client) CompleteUSBCopySession(ctx context.Context, runID string, jobID string) error {
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

	args := c.buildUSBCopyArgs("complete", jobID)
	args = append(args, "--run-id", runID)

	c.logger.Debugw("Completing USB copy lineage session", "args", args)

	cmd := exec.CommandContext(ctx, c.config.Lineage.CopyExecutable, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to complete USB copy lineage session: %w, output: %s", err, string(output))
	}

	return nil
}

// CancelUSBCopySession cancels a USB copy lineage session
func (c *Client) CancelUSBCopySession(ctx context.Context, runID string, jobID string) error {
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

	args := c.buildUSBCopyArgs("cancel", jobID)
	args = append(args, "--run-id", runID)

	c.logger.Infow("Cancelling USB copy lineage session", "runID", runID)

	cmd := exec.CommandContext(ctx, c.config.Lineage.CopyExecutable, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to cancel USB copy lineage session: %w, output: %s", err, string(output))
	}

	c.logger.Infow("USB copy lineage session cancelled", "runID", runID)
	return nil
}

// StartS3UploadSession starts a S3 upload lineage session
func (c *Client) StartS3UploadSession(ctx context.Context, sourceDir string, jobID string) (string, error) {
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

	// Load metadata from source directory (reuse the same metadata from copy session)
	if err := c.loadMetaData(sourceDir); err != nil {
		c.logger.Errorw("Failed to load metadata from source directory", "error", err, "sourceDir", sourceDir)
		return "", fmt.Errorf("failed to load metadata: %w", err)
	}

	args := c.buildS3UploadArgs("start", jobID)

	c.logger.Debugw("Starting S3 upload lineage session", "args", args)

	cmd := exec.CommandContext(ctx, c.config.Lineage.UploadExecutable, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to start S3 upload lineage session: %w, output: %s", err, string(output))
	}

	runID := strings.TrimSpace(string(output))

	return runID, nil
}

// CompleteS3UploadSession completes a S3 upload lineage session
func (c *Client) CompleteS3UploadSession(ctx context.Context, runID string, jobID string) error {
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

	args := c.buildS3UploadArgs("complete", jobID)
	args = append(args, "--run-id", runID)

	c.logger.Debugw("Completing S3 upload lineage session", "args", args)

	cmd := exec.CommandContext(ctx, c.config.Lineage.UploadExecutable, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to complete S3 upload lineage session: %w, output: %s", err, string(output))
	}

	return nil
}

// CancelS3UploadSession cancels a S3 upload lineage session
func (c *Client) CancelS3UploadSession(ctx context.Context, runID string, jobID string) error {
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

	args := c.buildS3UploadArgs("cancel", jobID)
	args = append(args, "--run-id", runID)

	c.logger.Debugw("Cancelling S3 upload lineage session", "args", args)

	cmd := exec.CommandContext(ctx, c.config.Lineage.UploadExecutable, args...)
	output, err := cmd.CombinedOutput()
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

// buildUSBCopyArgs builds common arguments
func (c *Client) buildUSBCopyArgs(command string, jobID string) []string {
	args := []string{command}

	// Add common arguments
	args = append(args, "--namespace", c.config.Lineage.Namespace)
	args = append(args, "--job-name", "usb-data-copy")
	args = append(args, "--robot-id", c.robotID)
	args = append(args, "--location", c.locationName)
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
func (c *Client) buildS3UploadArgs(command string, jobID string) []string {
	args := []string{command}

	// Add common arguments
	args = append(args, "--namespace", c.config.Lineage.Namespace)
	args = append(args, "--job-name", "s3-data-upload")
	args = append(args, "--robot-id", c.robotID)
	args = append(args, "--location", c.locationName)
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

// loadMetaData loads metadata from meta.json in the source directory
// Supports both v1.0 and v1.1+ metadata formats
func (c *Client) loadMetaData(sourceDir string) error {
	// Find meta.json file in the source directory
	metaPath, err := FindMetadataFile(sourceDir)
	if err != nil {
		return fmt.Errorf("failed to find metadata file: %w", err)
	}

	// Read and parse metadata using the existing function
	metadataInfo, err := ReadMetadata(metaPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	// Set values from parsed metadata
	c.robotID = metadataInfo.RobotID
	c.locationName = metadataInfo.Location

	c.logger.Infow("Loaded metadata from meta.json",
		"robot_id", c.robotID,
		"location_name", c.locationName,
		"version", metadataInfo.Version,
		"source_dir", sourceDir)

	return nil
}
