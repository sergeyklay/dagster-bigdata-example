# Support Ticket Analysis Pipeline

An example of a Dagster-based pipeline for processing 10 million synthetic (fake) support tickets using text embeddings and clustering analysis.

## Overview

This pipeline demonstrates a three-asset workflow for big data processing:

1. **Synthetic Dataset Generation** - Creates 10M support tickets with realistic data
2. **Text Embedding** - Converts ticket messages to 384-dimensional vectors
3. **Incremental Clustering** - Groups tickets into 20 topics using MiniBatchKMeans

## Architecture

```
Synthetic Support Tickets (50 partitions × 200k rows)
         ↓
Text Embeddings (SentenceTransformer 'all-MiniLM-L6-v2')
         ↓
Clustering Model Training (MiniBatchKMeans, 20 clusters)
         ↓
Cluster Assignment & Business Analysis
```

## Code Structure

The pipeline follows clean architecture principles with clear separation of concerns:

```
dbe/
├── __init__.py
├── assets.py          # Dagster orchestration logic
├── definitions.py     # Asset and resource definitions
├── resources.py       # S3 I/O manager and resources
└── logic.py          # Pure business logic (NEW!)
```

### Business Logic Extraction

All ticket generation business logic has been extracted into `logic.py`:

- **`SupportTicketGenerator`** - Main class for generating realistic tickets
- **Message Templates** - Product-specific message patterns
- **Product Configurations** - Error codes, features, and realistic data per product line
- **Pure Functions** - Testable, maintainable, and reusable logic

This separation ensures:
- ✅ **Clean Code**: Dagster assets focus only on orchestration
- ✅ **Testability**: Business logic can be tested independently
- ✅ **Maintainability**: Easy to modify ticket generation without touching Dagster
- ✅ **Reusability**: Logic can be used outside of Dagster if needed

## Dataset Schema

### Support Tickets
- `ticket_id`: Unique identifier (int)
- `customer_id`: Customer ID from 1-1,000,000 (int)
- `product_line`: "Payment System" | "Mobile App" | "API Integration"
- `message_text`: Realistic support ticket content (string)
- `created_at`: Timestamp within last 30 days (datetime)

### Embeddings
- `ticket_id`: Links to original ticket (int)
- `emb_0` to `emb_383`: 384-dimensional embedding vector (float)

### Clusters
- `ticket_id`: Links to original ticket (int)
- `cluster_id`: Assigned cluster 0-19 (int)

## Infrastructure

- **Storage**: MinIO (S3-compatible) for data persistence
- **Format**: Parquet files for efficient columnar storage
- **Partitioning**: 50 partitions of 200k rows each (~100MB per file)
- **Memory**: Designed for 4-8GB RAM machines

## Setup & Usage

### 1. Start Infrastructure

```bash
# Start MinIO storage
docker-compose up -d

# Verify MinIO is running
docker-compose ps
```

### 2. Install Dependencies

```bash
# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### 3. Run the Pipeline

```bash
# Start Dagster UI
dagster dev

# or
make serve

# Open browser to http://localhost:3000
# Navigate to Assets view
# Materialize individual partitions or all assets
```

### 4. Monitor Progress

The pipeline provides detailed logging:
- Data generation progress per partition
- Embedding computation with batch progress
- Clustering model training metrics
- Cluster distribution analysis

## Pipeline Assets

### Asset 1: `synthetic_support_tickets`
**Partitioned**: 50 partitions (partition_00 to partition_49)
- Generates realistic support ticket data using extracted business logic
- Product-specific message templates for each product line
- Reproducible with seed=42
- **Performance**: 200k tickets generated in ~4 seconds per partition

### Asset 2: `ticket_embeddings`
**Partitioned**: 50 partitions
- Processes one partition at a time
- Uses SentenceTransformer 'all-MiniLM-L6-v2'
- Batch size: 128 for memory efficiency
- Output: 384-dimensional vectors
- **Performance**: ~30 seconds per partition for embedding generation

### Asset 3: `trained_clustering_model`
**Unpartitioned**: Single model
- MiniBatchKMeans with 20 clusters
- Incremental learning with partial_fit()
- Batch size: 20,000 for training stability
- Automatically handles available partitions

### Asset 4: `ticket_clusters`
**Partitioned**: 50 partitions
- Assigns cluster IDs using trained model
- Provides cluster distribution per partition
- Perfect load balancing across 20 clusters

### Asset 5: `cluster_analysis`
**Unpartitioned**: Summary analysis
- Business insights and recommendations
- Processing statistics
- Saved as JSON in S3

## Business Value

### 🎯 **Automated Categorization**
Groups similar tickets into 20 semantic topics for efficient handling.

### ⚡ **Priority Routing**
- **Payment System** issues (clusters 0-6) → Financial team
- **Mobile App** crashes (clusters 7-13) → Mobile development
- **API Integration** problems (clusters 14-19) → Infrastructure team

### 📊 **Actionable Insights**
- Identify most common issue patterns
- Detect emerging problems early
- Optimize support team allocation

### 💾 **Resource Efficient**
- Disk-backed processing for large datasets
- Chunked processing fits in 4-8GB RAM
- Incremental learning reduces compute requirements

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Total Records | 10,000,000 |
| Partitions | 50 |
| Records/Partition | 200,000 |
| File Size | ~100MB per partition |
| Embedding Dimensions | 384 |
| Clusters | 20 |
| Data Generation | ~4 seconds per partition |
| Embedding Generation | ~30 seconds per partition |
| Estimated Total Runtime | 2-4 hours |

## Storage Layout

```
s3://dbe/
├── synthetic_support_tickets/
│   ├── partition_00.parquet
│   ├── partition_01.parquet
│   └── ... (50 files)
├── ticket_embeddings/
│   ├── partition_00.parquet
│   ├── partition_01.parquet
│   └── ... (50 files)
├── clustering/
│   └── trained_model.pkl
├── ticket_clusters/
│   ├── partition_00.parquet
│   ├── partition_01.parquet
│   └── ... (50 files)
└── analysis/
    └── cluster_analysis_results.json
```

## Development

### Code Quality
The codebase follows clean code principles:
- **Separation of Concerns**: Business logic separated from orchestration
- **Single Responsibility**: Each module has a focused purpose
- **Testability**: Pure functions and clear interfaces
- **Maintainability**: Well-documented and organized code

### Run Tests
```bash
# Install test dependencies
uv sync --group dev

# Run linting
make lint

# Test business logic independently
python -c "from dbe.logic import generate_partition_tickets; print('✅ Logic works!')"

# Check for issues
git diff --check
```

### Extend the Pipeline

1. **Modify Ticket Generation**: Edit `dbe/logic.py` to change message templates or product lines
2. **Add New Asset Types**: Extend the schema in `assets.py`
3. **Custom Clustering**: Modify MiniBatchKMeans parameters in `assets.py`
4. **Different Embeddings**: Replace SentenceTransformer model
5. **Alternative Storage**: Modify IOManager in `resources.py`

## Dependencies

Core dependencies for the pipeline:

- **dagster>=1.10.21** - Orchestration framework
- **dagster-aws>=0.26.21** - S3 integration
- **sentence-transformers>=4.1.0** - Text embeddings
- **scikit-learn>=1.7.0** - Machine learning
- **faker>=37.4.0** - Synthetic data generation
- **pandas>=2.3.0** - Data manipulation
- **pyarrow>=20.0.0** - Parquet file handling
- **boto3>=1.35.0** - AWS/S3 client

## Troubleshooting

### Common Issues

1. **MinIO Storage Full**:
   - Check disk space: `df -h`
   - Clean up old data: `docker-compose down -v`

2. **Memory Issues**:
   - Reduce batch sizes in `assets.py`
   - Process fewer partitions at once

3. **Import Errors**:
   - Ensure virtual environment is activated
   - Run `uv sync` to install dependencies

### Performance Tips

- **Parallel Processing**: Materialize multiple partitions simultaneously in Dagster UI
- **Memory Optimization**: Adjust batch sizes based on available RAM
- **Storage Optimization**: Use compression in Parquet files for better performance

---

Built with ❤️ using Dagster, SentenceTransformers, and scikit-learn.
