package jobs

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
)

// JobType represents the type of job (copy or upload)
type JobType string

const (
	// JobTypeCopy represents a file copy job
	JobTypeCopy JobType = "copy"

	// JobTypeUpload represents an S3 upload job
	JobTypeUpload JobType = "upload"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	// JobStatusQueued indicates the job is queued
	JobStatusQueued JobStatus = "queued"

	// JobStatusInProgress indicates the job is in progress
	JobStatusInProgress JobStatus = "in_progress"

	// JobStatusPaused indicates the job is paused
	JobStatusPaused JobStatus = "paused"

	// JobStatusDone indicates the job is completed successfully
	JobStatusDone JobStatus = "done"

	// JobStatusError indicates the job failed with an error
	JobStatusError JobStatus = "error"

	// JobStatusCancelled indicates the job is cancelled
	JobStatusCancelled JobStatus = "cancelled"
)

// Job represents a copy or upload job
type Job struct {
	ID            string            `db:"id"`
	Type          JobType           `db:"type"`
	Source        string            `db:"source"`
	Destination   string            `db:"destination"`
	Status        JobStatus         `db:"status"`
	Progress      float64           `db:"progress"`
	CreatedAt     time.Time         `db:"created_at"`
	UpdatedAt     time.Time         `db:"updated_at"`
	ErrorMsg      sql.NullString    `db:"error_message"` // Reverted to sql.NullString
	FileCount     int               `db:"file_count"`
	TotalSize     int64             `db:"total_size"`
	ProcessedSize int64             `db:"processed_size"`
	Metadata      map[string]string `db:"-"`
	RetryCount    int               `db:"-"`
}

// NewJob creates a new job with the specified type, source, and destination.
// It initializes the job's ID, status, and timestamps.
// The job is not saved to the database by this function.
func NewJob(jobType JobType, source, destination string) *Job {
	return &Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Source:      source,
		Destination: destination,
		Status:      JobStatusQueued,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Metadata:    make(map[string]string),
	}
}

// UpdateProgress updates the job's progress and marks it as updated.
// It does not save the job to the database.
func (j *Job) UpdateProgress(progress float64) {
	j.Progress = progress
	j.UpdatedAt = time.Now()
}

// UpdateStatus updates the job's status and marks it as updated.
// It does not save the job to the database.
func (j *Job) UpdateStatus(status JobStatus) {
	j.Status = status
	j.UpdatedAt = time.Now()
}

// SetError sets the job's error message and status to error.
// It does not save the job to the database.
func (j *Job) SetError(err error) {
	if err != nil {
		j.ErrorMsg = sql.NullString{String: err.Error(), Valid: true}
	} else {
		j.ErrorMsg = sql.NullString{Valid: false}
	}
	j.Status = JobStatusError
	j.UpdatedAt = time.Now()
}

// AddMetadata adds a key-value pair to the job's metadata.
func (j *Job) AddMetadata(key, value string) {
	if j.Metadata == nil {
		j.Metadata = make(map[string]string)
	}
	j.Metadata[key] = value
}

// GetMetadata returns the job's metadata.
// This implements the JobProvider interface for lineage integration.
func (j *Job) GetMetadata() map[string]string {
	return j.Metadata
}

// GetID returns the job's ID.
// This implements the JobProvider interface for lineage integration.
func (j *Job) GetID() string {
	return j.ID
}

// GetSource returns the job's source path.
// This implements the JobProvider interface for lineage integration.
func (j *Job) GetSource() string {
	return j.Source
}

// GetDestination returns the job's destination path.
// This implements the JobProvider interface for lineage integration.
func (j *Job) GetDestination() string {
	return j.Destination
}

// GetCreatedAt returns the job's creation time.
// This implements the JobProvider interface for lineage integration.
func (j *Job) GetCreatedAt() time.Time {
	return j.CreatedAt
}

// JobPersister defines an interface for saving and potentially retrieving jobs.
// This helps to break import cycles between jobs and storage packages.
type JobPersister interface {
	SaveJob(job *Job) error
	SaveJobFiles(jobFiles []*JobFile) error
	GetCompletedFiles(jobID string) ([]*JobFile, error)
	SaveJobFile(jobFile *JobFile) error
	// GetJob(id string) (*Job, error) // Example: if workers needed to fetch/reload jobs
}

// JobFile represents a file being tracked within a job
type JobFile struct {
	JobID        string
	FilePath     string
	RelativePath string
	Size         int64
	Status       string
	Checksum     string
	CompletedAt  *time.Time
	ErrorMessage string
}

// ProgressReader is an io.Reader wrapper that calls a callback function on each Read operation
// to report progress.
type ProgressReader struct {
	Reader     io.Reader
	Size       int64 // Total size of the file/stream
	BytesRead  int64 // Bytes read so far
	OnProgress func(bytesRead int64, totalBytes int64)
	mu         sync.Mutex
}

// Read implements the io.Reader interface for ProgressReader
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	pr.mu.Lock()
	pr.BytesRead += int64(n)
	// Capture values under lock to ensure consistency if OnProgress is slow
	currentBytesRead := pr.BytesRead
	totalSize := pr.Size
	pr.mu.Unlock()

	if pr.OnProgress != nil {
		pr.OnProgress(currentBytesRead, totalSize)
	}
	return
}

// String returns a string representation of the job
func (j *Job) String() string {
	return fmt.Sprintf("Job{ID: %s, Type: %s, Status: %s, Progress: %.2f%%}",
		j.ID, j.Type, j.Status, j.Progress*100)
}

// Clone returns a copy of the job
func (j *Job) Clone() *Job {
	clone := &Job{
		ID:            j.ID,
		Type:          j.Type,
		Source:        j.Source,
		Destination:   j.Destination,
		Status:        j.Status,
		Progress:      j.Progress,
		CreatedAt:     j.CreatedAt,
		UpdatedAt:     j.UpdatedAt,
		ErrorMsg:      j.ErrorMsg,
		FileCount:     j.FileCount,
		TotalSize:     j.TotalSize,
		ProcessedSize: j.ProcessedSize,
		Metadata:      make(map[string]string),
	}

	// Copy metadata
	for k, v := range j.Metadata {
		clone.Metadata[k] = v
	}

	return clone
}
