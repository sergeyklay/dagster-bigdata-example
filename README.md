# Dagster BigData Example with S3 Integration

This educational project demonstrates how to use Dagster with S3-compatible storage (Minio) for data processing pipelines.

## Architecture

- **Dagster**: Orchestration and asset management
- **Minio**: S3-compatible object storage running locally
- **Pandas**: Data processing
- **S3PickleIOManager**: Handles asset storage in S3

## Setup Instructions

### 1. Start Minio Service

First, start the Minio service using Docker Compose:

```bash
docker-compose up -d
```

This will start Minio on:
- **S3 API**: http://localhost:9000
- **Web Console**: http://localhost:9001
- **Credentials**: minio / miniosecret

### 2. Install Dependencies

Install the Python dependencies:

```bash
uv sync
```

### 3. Run Dagster Pipeline

Start the Dagster development server:

```bash
dagster dev -m dbe.definitions
```

Or using the serve script:

```bash
./serve.sh
```

## Assets

The pipeline includes three main assets:

1. **`sample_data`**: Loads data from `data/sample_data.csv`
2. **`processed_data`**: Adds age group classification to the data
3. **`processed_data_csv`**: Saves the processed data as CSV to S3

All assets use the S3PickleIOManager for intermediate storage, with the final CSV being explicitly saved to S3.

## S3 Configuration

The S3 configuration is set up to work with the local Minio instance:

```python
s3_resource = S3Resource(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="miniosecret",
    region_name="us-east-1",
)
```

## Accessing Results

### Via Minio Web Console

1. Open http://localhost:9001 in your browser
2. Login with: minio / miniosecret
3. Navigate to the 'dbe' bucket to see your data

### Via S3 API

You can also access the data programmatically using boto3 or AWS CLI configured with the Minio endpoint.

## Development

### Project Structure

```
├── dagster/
│   └── dagster.yaml       # Dagster instance configuration
├── dbe/
│   ├── __init__.py
│   ├── assets.py          # Dagster assets with S3 integration
│   ├── definitions.py     # Dagster definitions (assets + resources)
│   └── resources.py       # S3 and IO manager resource definitions
├── data/
│   └── sample_data.csv    # Input data
├── docker-compose.yml     # Minio service definition
├── serve.sh               # Development server startup script
├── Makefile               # Build and utility commands
├── pyproject.toml         # Dependencies and project configuration
└── uv.lock                # Dependency lock file
```

### Key Features

- **S3-Compatible Storage**: Uses Minio for local S3-like storage
- **Asset Dependencies**: Proper asset dependency management
- **Hybrid Storage**: Combines S3PickleIOManager for intermediate data and direct S3 upload for final CSV
- **Local Development**: Everything runs locally with Docker

### Next Steps

You can extend this setup by:
- Adding more complex data transformations
- Implementing partitioned assets
- Adding data quality checks
- Integrating with other data sources
- Setting up production deployment with real AWS S3
