package storage

import (
	"database/sql"
	"fmt"

	j "github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
)

// SaveJobFile saves or updates a file's tracking information
// This ensures DB implements the JobPersister interface from jobs package
func (d *DB) SaveJobFile(jobFile *j.JobFile) error {
	_, err := d.db.Exec(`
		INSERT INTO job_files (
			job_id, file_path, relative_path, size, status, 
			checksum, completed_at, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id, file_path) DO UPDATE SET
			status = excluded.status,
			checksum = excluded.checksum,
			completed_at = excluded.completed_at,
			error_message = excluded.error_message
	`,
		jobFile.JobID, jobFile.FilePath, jobFile.RelativePath, jobFile.Size,
		jobFile.Status, jobFile.Checksum, jobFile.CompletedAt, jobFile.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to save job file: %w", err)
	}
	return nil
}

// SaveJobFiles saves multiple job files in a transaction
func (d *DB) SaveJobFiles(jobFiles []*j.JobFile) error {
	if len(jobFiles) == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO job_files (
			job_id, file_path, relative_path, size, status, 
			checksum, completed_at, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id, file_path) DO UPDATE SET
			status = excluded.status,
			checksum = excluded.checksum,
			completed_at = excluded.completed_at,
			error_message = excluded.error_message
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, jobFile := range jobFiles {
		_, err = stmt.Exec(
			jobFile.JobID, jobFile.FilePath, jobFile.RelativePath, jobFile.Size,
			jobFile.Status, jobFile.Checksum, jobFile.CompletedAt, jobFile.ErrorMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to save job file %s: %w", jobFile.FilePath, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetJobFiles retrieves all files for a specific job
func (d *DB) GetJobFiles(jobID string) ([]*j.JobFile, error) {
	rows, err := d.db.Query(`
		SELECT job_id, file_path, relative_path, size, status, 
			checksum, completed_at, error_message
		FROM job_files
		WHERE job_id = ?
		ORDER BY file_path
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job files: %w", err)
	}
	defer rows.Close()

	var files []*j.JobFile
	for rows.Next() {
		file := &j.JobFile{}
		var completedAt sql.NullTime
		var checksum, errorMsg sql.NullString

		err := rows.Scan(
			&file.JobID, &file.FilePath, &file.RelativePath, &file.Size,
			&file.Status, &checksum, &completedAt, &errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job file: %w", err)
		}

		if completedAt.Valid {
			file.CompletedAt = &completedAt.Time
		}
		if checksum.Valid {
			file.Checksum = checksum.String
		}
		if errorMsg.Valid {
			file.ErrorMessage = errorMsg.String
		}

		files = append(files, file)
	}

	return files, nil
}

// GetPendingFiles retrieves files that haven't been processed yet
func (d *DB) GetPendingFiles(jobID string) ([]*j.JobFile, error) {
	rows, err := d.db.Query(`
		SELECT job_id, file_path, relative_path, size, status, 
			checksum, completed_at, error_message
		FROM job_files
		WHERE job_id = ? AND status = 'pending'
		ORDER BY file_path
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}
	defer rows.Close()

	var files []*j.JobFile
	for rows.Next() {
		file := &j.JobFile{}
		var completedAt sql.NullTime
		var checksum, errorMsg sql.NullString

		err := rows.Scan(
			&file.JobID, &file.FilePath, &file.RelativePath, &file.Size,
			&file.Status, &checksum, &completedAt, &errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pending file: %w", err)
		}

		if checksum.Valid {
			file.Checksum = checksum.String
		}
		if errorMsg.Valid {
			file.ErrorMessage = errorMsg.String
		}

		files = append(files, file)
	}

	return files, nil
}

// GetCompletedFiles retrieves files that have been successfully processed
func (d *DB) GetCompletedFiles(jobID string) ([]*j.JobFile, error) {
	rows, err := d.db.Query(`
		SELECT job_id, file_path, relative_path, size, status, 
			checksum, completed_at, error_message
		FROM job_files
		WHERE job_id = ? AND status = 'completed'
		ORDER BY file_path
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get completed files: %w", err)
	}
	defer rows.Close()

	var files []*j.JobFile
	for rows.Next() {
		file := &j.JobFile{}
		var completedAt sql.NullTime
		var checksum, errorMsg sql.NullString

		err := rows.Scan(
			&file.JobID, &file.FilePath, &file.RelativePath, &file.Size,
			&file.Status, &checksum, &completedAt, &errorMsg,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan completed file: %w", err)
		}

		if completedAt.Valid {
			file.CompletedAt = &completedAt.Time
		}
		if checksum.Valid {
			file.Checksum = checksum.String
		}

		files = append(files, file)
	}

	return files, nil
}

// ResetJobFiles removes all file tracking for a job
func (d *DB) ResetJobFiles(jobID string) error {
	_, err := d.db.Exec("DELETE FROM job_files WHERE job_id = ?", jobID)
	if err != nil {
		return fmt.Errorf("failed to reset job files: %w", err)
	}
	return nil
}

// GetFileTrackingStats returns statistics about file tracking for a job
func (d *DB) GetFileTrackingStats(jobID string) (total, completed, failed int, err error) {
	row := d.db.QueryRow(`
		SELECT 
			COUNT(*) as total,
			SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
			SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
		FROM job_files
		WHERE job_id = ?
	`, jobID)

	err = row.Scan(&total, &completed, &failed)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to get file tracking stats: %w", err)
	}

	return total, completed, failed, nil
}