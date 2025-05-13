package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Common errors
var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueEmpty  = errors.New("queue is empty")
	ErrJobNotFound = errors.New("job not found")
)

// Queue represents a job queue
type Queue struct {
	name       string
	jobs       []*Job
	active     *Job
	mu         sync.Mutex
	jobAdded   chan struct{}
	closed     bool
	maxRetries int
	logger     *zap.SugaredLogger
}

// NewQueue creates a new job queue with the specified name
func NewQueue(name string, maxRetries int, logger *zap.SugaredLogger) *Queue {
	return &Queue{
		name:       name,
		jobs:       make([]*Job, 0),
		maxRetries: maxRetries,
		logger:     logger,
		jobAdded:   make(chan struct{}, 1),
	}
}

// Enqueue adds a job to the queue
func (q *Queue) Enqueue(job *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.jobs = append(q.jobs, job)
	q.logger.Infow("Job enqueued",
		"queue", q.name,
		"jobID", job.ID,
		"jobType", job.Type,
		"source", job.Source,
		"destination", job.Destination)

	// Signal that a job is available
	select {
	case q.jobAdded <- struct{}{}:
	default:
		// Channel already has a notification
	}

	return nil
}

// Dequeue removes and returns the next job from the queue
func (q *Queue) Dequeue() (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	if len(q.jobs) == 0 {
		return nil, ErrQueueEmpty
	}

	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	q.active = job

	q.logger.Infow("Job dequeued",
		"queue", q.name,
		"jobID", job.ID,
		"jobType", job.Type)

	return job, nil
}

// Wait blocks until a job is available or the context is canceled
func (q *Queue) Wait(ctx context.Context) error {
	for {
		// Check if there are jobs available
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrQueueClosed
		}

		if len(q.jobs) > 0 {
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()

		// Wait for a job to be added or context to be canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.jobAdded:
			// A job was added, check again
		}
	}
}

// Size returns the number of jobs in the queue
func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// Close closes the queue
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	close(q.jobAdded)

	q.logger.Infow("Queue closed", "queue", q.name)
}

// GetActiveJob returns the currently active job
func (q *Queue) GetActiveJob() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.active
}

// ClearActiveJob clears the currently active job
func (q *Queue) ClearActiveJob() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active = nil
}

// GetJobs returns a copy of all jobs in the queue
func (q *Queue) GetJobs() []*Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	jobs := make([]*Job, len(q.jobs))
	for i, job := range q.jobs {
		jobs[i] = job.Clone()
	}

	return jobs
}

// FindJob finds a job by ID
func (q *Queue) FindJob(id string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check active job first
	if q.active != nil && q.active.ID == id {
		return q.active.Clone(), nil
	}

	// Check queued jobs
	for _, job := range q.jobs {
		if job.ID == id {
			return job.Clone(), nil
		}
	}

	return nil, ErrJobNotFound
}

// RemoveJob removes a job from the queue by ID
func (q *Queue) RemoveJob(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	for i, job := range q.jobs {
		if job.ID == id {
			// Remove the job from the queue
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)

			q.logger.Infow("Job removed from queue",
				"queue", q.name,
				"jobID", id)

			return nil
		}
	}

	return ErrJobNotFound
}

// RetryJob adds a job back to the queue for retry
func (q *Queue) RetryJob(job *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	// Check if we've exceeded max retries
	retryCountStr, hasRetryCount := job.Metadata["retry_count"] // Direct map access
	var count int
	if hasRetryCount {
		count = 1 // Default to 1 if parsing fails
		_, _ = fmt.Sscanf(retryCountStr, "%d", &count)
		count++
	} else {
		count = 1
	}

	// Update retry count
	job.AddMetadata("retry_count", fmt.Sprintf("%d", count))

	if count > q.maxRetries {
		q.logger.Warnw("Job exceeded max retries",
			"queue", q.name,
			"jobID", job.ID,
			"retryCount", count,
			"maxRetries", q.maxRetries)

		job.SetError(errors.New("exceeded maximum retry count"))
		return errors.New("exceeded maximum retry count")
	}

	// Add retry delay metadata
	job.AddMetadata("retry_at", time.Now().Add(getBackoffDuration(count)).Format(time.RFC3339))

	// Reset job status
	job.UpdateStatus(JobStatusQueued)
	job.UpdateProgress(0)

	// Add to the beginning of the queue for faster retry
	q.jobs = append([]*Job{job}, q.jobs...)

	q.logger.Infow("Job queued for retry",
		"queue", q.name,
		"jobID", job.ID,
		"retryCount", count)

	// Signal that a job is available
	select {
	case q.jobAdded <- struct{}{}:
	default:
		// Channel already has a notification
	}

	return nil
}

// getBackoffDuration calculates exponential backoff duration
func getBackoffDuration(retryCount int) time.Duration {
	// Base delay of 5 seconds with exponential backoff
	// 5s, 10s, 20s, 40s, etc.
	baseDelay := 5 * time.Second
	maxDelay := 5 * time.Minute

	// Calculate exponential backoff
	delay := baseDelay * time.Duration(1<<uint(retryCount-1))

	// Cap at max delay
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}
