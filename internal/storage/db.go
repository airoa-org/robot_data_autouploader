package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	j "github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs" // Explicit alias
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

// DB represents the database connection
type DB struct {
	db     *sql.DB
	logger *zap.SugaredLogger
}

// NewDB creates a new database connection
func NewDB(dbPath string, logger *zap.SugaredLogger) (*DB, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Open database connection
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable WAL mode for better concurrency
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	// Set busy timeout to 5 seconds
	_, err = db.Exec("PRAGMA busy_timeout = 5000;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// Set connection parameters
	db.SetMaxOpenConns(1) // SQLite only supports one writer at a time
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	// Ping database to verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{
		db:     db,
		logger: logger,
	}, nil
}

// Close closes the database connection
func (d *DB) Close() error {
	return d.db.Close()
}

// Migrate creates the necessary tables
func (d *DB) Migrate() error {
	d.logger.Info("Migrating database schema")

	// Create jobs table
	_, err := d.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			source TEXT NOT NULL,
			destination TEXT NOT NULL,
			status TEXT NOT NULL,
			progress REAL DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			error_message TEXT,
			file_count INTEGER DEFAULT 0,
			total_size INTEGER DEFAULT 0,
			processed_size INTEGER DEFAULT 0,
			metadata TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create jobs table: %w", err)
	}

	// Create job_metadata table
	_, err = d.db.Exec(`
		CREATE TABLE IF NOT EXISTS job_metadata (
			job_id TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (job_id, key),
			FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create job_metadata table: %w", err)
	}

	// Create config table
	_, err = d.db.Exec(`
		CREATE TABLE IF NOT EXISTS config (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create config table: %w", err)
	}

	d.logger.Info("Database schema migrated")

	return nil
}

// SaveJob saves a job to the database
func (d *DB) SaveJob(job *j.Job) error { // Use alias j.Job
	// Begin transaction
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert or update job
	_, err = tx.Exec(`
		INSERT INTO jobs (
			id, type, source, destination, status, progress, 
			created_at, updated_at, error_message, 
			file_count, total_size, processed_size
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			progress = excluded.progress,
			updated_at = excluded.updated_at,
			error_message = excluded.error_message,
			file_count = excluded.file_count,
			total_size = excluded.total_size,
			processed_size = excluded.processed_size
	`,
		job.ID, job.Type, job.Source, job.Destination, job.Status, job.Progress,
		job.CreatedAt, job.UpdatedAt, job.ErrorMsg,
		job.FileCount, job.TotalSize, job.ProcessedSize,
	)
	if err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}

	// Delete existing metadata
	_, err = tx.Exec("DELETE FROM job_metadata WHERE job_id = ?", job.ID)
	if err != nil {
		return fmt.Errorf("failed to delete job metadata: %w", err)
	}

	// Insert metadata
	for key, value := range job.Metadata {
		_, err = tx.Exec(
			"INSERT INTO job_metadata (job_id, key, value) VALUES (?, ?, ?)",
			job.ID, key, value,
		)
		if err != nil {
			return fmt.Errorf("failed to save job metadata: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetJob retrieves a job from the database
func (d *DB) GetJob(id string) (*j.Job, error) { // Use alias j.Job
	// Query job
	row := d.db.QueryRow(`
		SELECT id, type, source, destination, status, progress, 
			created_at, updated_at, error_message, 
			file_count, total_size, processed_size
		FROM jobs
		WHERE id = ?
	`, id)

	// Parse job
	var job j.Job // Use alias j.Job
	var createdAt, updatedAt string
	err := row.Scan(
		&job.ID, &job.Type, &job.Source, &job.Destination, &job.Status, &job.Progress,
		&createdAt, &updatedAt, &job.ErrorMsg,
		&job.FileCount, &job.TotalSize, &job.ProcessedSize,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Parse timestamps
	job.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	job.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)

	// Initialize metadata
	job.Metadata = make(map[string]string)

	// Query metadata
	rows, err := d.db.Query("SELECT key, value FROM job_metadata WHERE job_id = ?", id)
	if err != nil {
		return nil, fmt.Errorf("failed to get job metadata: %w", err)
	}
	defer rows.Close()

	// Parse metadata
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("failed to parse job metadata: %w", err)
		}
		job.Metadata[key] = value
	}

	return &job, nil
}

// GetAllJobs retrieves all jobs from the database, including past jobs.
func (d *DB) GetAllJobs() ([]*j.Job, error) {
	rows, err := d.db.Query(`
	   SELECT id, type, source, destination, status, progress, 
		   created_at, updated_at, error_message, 
		   file_count, total_size, processed_size
	   FROM jobs
	   ORDER BY created_at DESC
   `)
	if err != nil {
		return nil, fmt.Errorf("failed to get all jobs: %w", err)
	}
	var jobsList []*j.Job
	var jobIDs []string
	for rows.Next() {
		job := &j.Job{}
		var createdAt, updatedAt string
		err := rows.Scan(
			&job.ID, &job.Type, &job.Source, &job.Destination, &job.Status, &job.Progress,
			&createdAt, &updatedAt, &job.ErrorMsg,
			&job.FileCount, &job.TotalSize, &job.ProcessedSize,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to parse job: %w", err)
		}
		job.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		job.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		job.Metadata = make(map[string]string)
		jobsList = append(jobsList, job)
		jobIDs = append(jobIDs, job.ID)
	}
	rows.Close() // Now the connection is free for new queries

	// Now, for each job, load metadata
	for i, job := range jobsList {
		metaRows, err := d.db.Query("SELECT key, value FROM job_metadata WHERE job_id = ?", job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get job metadata: %w", err)
		}
		for metaRows.Next() {
			var key, value string
			if err := metaRows.Scan(&key, &value); err != nil {
				metaRows.Close()
				return nil, fmt.Errorf("failed to parse job metadata: %w", err)
			}
			jobsList[i].Metadata[key] = value
		}
		metaRows.Close()
	}

	return jobsList, nil
}

// GetPendingJobs retrieves all pending jobs of a specific type
func (d *DB) GetPendingJobs(jobType j.JobType) ([]*j.Job, error) { // Use alias j.JobType and []*j.Job
	// Query jobs
	rows, err := d.db.Query(`
		SELECT id, type, source, destination, status, progress, 
			created_at, updated_at, error_message, 
			file_count, total_size, processed_size
		FROM jobs
		WHERE type = ? AND (status = ? OR status = ?)
		ORDER BY created_at ASC
	`, jobType, j.JobStatusQueued, j.JobStatusPaused)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending jobs: %w", err)
	}
	defer rows.Close()

	// Parse jobs
	var jobsList []*j.Job // Use alias []*j.Job
	for rows.Next() {
		job := &j.Job{} // Use alias j.Job
		var createdAt, updatedAt string
		err := rows.Scan(
			&job.ID, &job.Type, &job.Source, &job.Destination, &job.Status, &job.Progress,
			&createdAt, &updatedAt, &job.ErrorMsg,
			&job.FileCount, &job.TotalSize, &job.ProcessedSize,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to parse job: %w", err)
		}

		// Parse timestamps
		job.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		job.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)

		// Initialize metadata
		job.Metadata = make(map[string]string)

		// Add job to list
		jobsList = append(jobsList, job)
	}

	// Load metadata for all jobs
	for _, job := range jobsList {
		// Query metadata
		metaRows, err := d.db.Query("SELECT key, value FROM job_metadata WHERE job_id = ?", job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get job metadata: %w", err)
		}

		// Parse metadata
		for metaRows.Next() {
			var key, value string
			if err := metaRows.Scan(&key, &value); err != nil {
				metaRows.Close()
				return nil, fmt.Errorf("failed to parse job metadata: %w", err)
			}
			job.Metadata[key] = value
		}
		metaRows.Close()
	}

	return jobsList, nil
}

// HasSuccessfullyCopiedJob checks if a copy job for the given source path
// has been completed successfully.
func (d *DB) HasSuccessfullyCopiedJob(sourcePath string) (bool, error) {
	var count int
	query := `SELECT COUNT(*) FROM jobs WHERE type = ? AND source = ? AND status = ?`
	err := d.db.QueryRow(query, j.JobTypeCopy, sourcePath, j.JobStatusDone).Scan(&count) // Use alias j.JobTypeCopy, j.JobStatusDone

	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		d.logger.Errorw("Failed to query for successfully copied job", "sourcePath", sourcePath, "error", err)
		return false, fmt.Errorf("failed to query jobs table for source '%s': %w", sourcePath, err)
	}
	return count > 0, nil
}

// GetJobsByStatus retrieves jobs that match any of the provided statuses
func (d *DB) GetJobsByStatus(statuses ...j.JobStatus) ([]*j.Job, error) {
	if len(statuses) == 0 {
		return nil, fmt.Errorf("at least one status must be provided")
	}

	// Build query with placeholders for each status
	placeholders := make([]string, len(statuses))
	args := make([]interface{}, len(statuses))
	for i, status := range statuses {
		placeholders[i] = "?"
		args[i] = status
	}

	query := fmt.Sprintf(`
		SELECT id, type, source, destination, status, progress, 
			created_at, updated_at, error_message, 
			file_count, total_size, processed_size
		FROM jobs
		WHERE status IN (%s)
		ORDER BY created_at ASC
	`, strings.Join(placeholders, ","))

	// Query jobs
	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs by status: %w", err)
	}
	defer rows.Close()

	// Parse jobs
	var jobsList []*j.Job
	for rows.Next() {
		job := &j.Job{}
		var createdAt, updatedAt string
		err := rows.Scan(
			&job.ID, &job.Type, &job.Source, &job.Destination, &job.Status, &job.Progress,
			&createdAt, &updatedAt, &job.ErrorMsg,
			&job.FileCount, &job.TotalSize, &job.ProcessedSize,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to parse job: %w", err)
		}

		// Parse timestamps
		job.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		job.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)

		// Initialize metadata
		job.Metadata = make(map[string]string)

		// Add job to list
		jobsList = append(jobsList, job)
	}

	// Load metadata for all jobs
	for _, job := range jobsList {
		// Query metadata
		metaRows, err := d.db.Query("SELECT key, value FROM job_metadata WHERE job_id = ?", job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get job metadata: %w", err)
		}

		// Parse metadata
		for metaRows.Next() {
			var key, value string
			if err := metaRows.Scan(&key, &value); err != nil {
				metaRows.Close()
				return nil, fmt.Errorf("failed to parse job metadata: %w", err)
			}
			job.Metadata[key] = value
		}
		metaRows.Close()
	}

	return jobsList, nil
}
