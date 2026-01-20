# Robot Data Autouploader

This system is designed to automatically upload data, from a local source (e.g., USB drive or specified directories) to a remote storage solution, such as an S3-compatible object store. It includes a daemon process for background monitoring and uploading, and a Terminal User Interface (TUI) for observing and managing the upload jobs.

## Running the System

The daemon and client can be downloaded from the [releases page](https://github.com/airoa-org/robot_data_autouploader/releases).

### Recommended Installation Method

Run the playbook for uploader machine in the `hsr_devops` repository.
Instructions for installation and operation can be found in the Data Collection Manual document.

This playbook includes the auto-USB mounting configuration.

### Prerequisites

- Linux x86_64 system
- airoa-lineage installed (for data-lineage)

### Setup data-lineage scripts

Install `python3`.

```sh
sudo apt update
sudo apt install -y --no-install-recommends \
    git \
    git-lfs \
    build-essential \
    python3 \
    python3-pip \
    python-is-python3
```

Install `airoa-lineage`.

```sh
pip install git+https://github.com/airoa-org/airoa-lineage
```

Check if the command is available.

```sh
airoa-lineage-usb-copy --version
airoa-lineage-s3-upload --version
```

### Add to config file

```yaml
lineage:
  enabled: true
  marquez_url: "https://url-for-marquez"
```

### Running the Daemon

#### Binary

The daemon is responsible for monitoring source directories or USB drives for new data, staging it, and uploading it to the configured storage solution.

```bash
./robot-data-daemon-linux-x86_64 --config <path_to_config_file>
```

#### Locally

```bash
go run cmd/daemon/main.go --config <path_to_config_file>
```

For testing with MinIO

```bash
docker compose -f testing/docker-compose.yml up -d
go run cmd/daemon/main.go --config testing/config-minio.yaml
```

### Running the TUI Client

The TUI client provides a user interface to view the status of ongoing and completed jobs.
It must point to the same database as the daemon.

When a upload job has failed, it can be retried from the TUI client.

#### Binary

```bash
./robot-data-client-linux-x86_64 --db <path_to_db_file>
```

#### Locally

```bash
go run cmd/client/main.go --db <path_to_db_file>
```

#### Screenshots 
<img width="1509" alt="Screenshot 2025-05-26 at 19 04 52" src="https://github.com/user-attachments/assets/63056938-aedf-4d5f-915f-67a3c379b80c" />

<img width="1505" alt="Screenshot 2025-05-26 at 19 05 05" src="https://github.com/user-attachments/assets/d65ee0bc-d01f-4a59-b006-965d2b8772db" />

<img width="631" alt="Screenshot 2025-05-26 at 19 05 14" src="https://github.com/user-attachments/assets/32c78075-c8a6-4d20-8278-2f94b2c68c57" />

## Development

### Prerequisites
- Go 1.24 or later
- SQLite3 (for the database)

### Building

This project includes build scripts that embed git repository information into the binaries.

#### Using the Build Script

The easiest way to build with embedded git information:

```bash
./build.sh
```

This will:
- Extract git information (hash, branch, tag, remote URL)
- Build both daemon and client binaries in the `bin/` directory
- Embed build information for version tracking

#### Using Make

```bash
# Build all binaries
make build

# Build individual components
make build-daemon
make build-client
```

#### Docker Build for Linux (from macOS)

```bash
docker run --rm --platform=linux/amd64 -v "$(pwd):/app" -w /app golang:1.24 sh -c "
  apt-get update && apt-get install -y libsqlite3-dev && 
  CGO_ENABLED=1 GOOS=linux GOARCH=amd64 ./build.sh
"
```

#### Version Information

After building, you can check the embedded version information:

```bash
./bin/robot_data_daemon -v
./bin/robot_data_client -v
```

## Configuration

The system is configured via a YAML file (e.g., `testing/config-minio.yaml`). Below is a comprehensive list of all available configuration options:

### Daemon Settings (`daemon`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_path` | string | `~/.autoloader/jobs.db` | Path to the SQLite database file for storing job information |
| `disable_directory_scan` | bool | `false` | Disable automatic directory scanning for new data |

### Storage Settings (`storage`)

#### S3 Configuration (`storage.s3`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | - | S3 bucket name for uploads |
| `endpoint` | string | - | S3 endpoint URL (for custom S3-compatible services like MinIO) |
| `region` | string | `us-west-1` | AWS region |
| `access_key` | string | - | S3 access key ID |
| `secret_key` | string | - | S3 secret access key |
| `upload_path` | string | - | Prefix path for uploaded objects in S3 |

#### Local Storage Configuration (`storage.local`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `staging_dir` | string | `~/.autoloader/staging` | Local directory for staging files before upload |
| `retention_policy_on_upload` | string | - | Action after successful upload (`delete` to remove staged files) |
| `retention_policy_on_copy` | string | - | Action after successful copy (`delete` to remove source files) |

### USB Settings (`usb`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_mount_point` | string | `/storage` | Mount point to monitor for USB devices |
| `target_directory` | string | - | Specific directory within USB devices to monitor |
| `ignored_patterns` | []string | `["SYSTEM*", ".*"]` | File/directory patterns to ignore during USB scanning |
| `scan_interval_ms` | int | `500` | Interval in milliseconds between USB scans |

### Job Settings (`jobs`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `direct_upload` | bool | `false` | Skip staging and upload directly from source |
| `max_retries` | int | `3` | Maximum number of retry attempts for failed jobs |

### Copy Settings (`copy`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `exclude_patterns` | []string | `["*.tmp", "*.DS_Store"]` | File patterns to exclude during copy operations |
| `exclude_directory_patterns` | []string | `["System Volume Information"]` | Directory patterns to exclude during copy operations |
| `min_free_space_ratio` | float64 | `0.05` | Minimum free space ratio to maintain (0.05 = 5%) |

### Upload Settings (`upload`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `parallelism` | int | `1` | Number of concurrent upload threads |
| `chunk_size_mb` | int | `8` | Chunk size in MB for multipart uploads |
| `throttle_mbps` | int | `50` | Upload speed limit in Mbps (0 = no limit) |
| `allowed_patterns` | []string | `["data.bag", "meta.json"]` | File patterns allowed for upload (empty = allow all) |

### Example Configuration

See `testing/config-minio.yaml` for a complete example configuration file that can be used with MinIO for testing purposes.
