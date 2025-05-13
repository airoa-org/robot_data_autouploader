# Robot Data Autouploader

This system is designed to automatically upload data, from a local source (e.g., USB drive or specified directories) to a remote storage solution, such as an S3-compatible object store. It includes a daemon process for background monitoring and uploading, and a Terminal User Interface (TUI) for observing and managing the upload jobs.

## Running the System

### Prerequisites

- Go (Developed on 1.24.1)
- Access to the configured S3 storage (if not using local/test setup)

### Running the Daemon

The daemon is responsible for monitoring source directories or USB drives for new data and managing the upload queue.

```bash
go run cmd/daemon/main.go --config <path_to_config_file>
# Example: go run cmd/daemon/main.go --config configs/config.yaml
# For testing with MinIO: go run cmd/daemon/main.go --config testing/config-minio.yaml
```

### Running the TUI Client

The TUI client provides a user interface to view the status of ongoing and completed jobs.
It must point to the same database as the daemon.

```bash
go run cmd/client/main.go --db <path_to_db_file>
```

## Building

### On MacOS

`docker run --rm --platform=linux/amd64 -v "$(pwd):/app" -w /app golang:1.24 sh -c "apt-get update && apt-get install -y libsqlite3-dev && CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o robot-data-daemon ./cmd/daemon/main.go"`

Make sure to build on linux/amd64, and by using a OS with glibc (e.g., Ubuntu) to avoid issues with CGO.

## Configuration

See `config.go` for the full list of configurations.
The system is configured via a YAML file (e.g., `configs/config.yaml` or `testing/config-minio.yaml`). Key configuration areas include:

-   **Daemon settings**: database path.
-   **Storage settings**: S3 credentials, bucket details, local staging directories, source patterns.
-   **USB settings**: Auto-mount options, ignored patterns, mount points.
-   **Job settings**: Upload behavior, retries.
-   **Copy settings**: Parallelism for copy operations.
