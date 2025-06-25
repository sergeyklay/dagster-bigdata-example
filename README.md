# Support Ticket Analysis Pipeline

An example of a Dagster pipeline for processing 10M synthetic support tickets using text embeddings and clustering.

## Setup

1. **Start MinIO**:
   ```bash
   docker-compose up -d
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Run pipeline**:
   ```bash
   make serve
   ```

4. **Open browser**: http://localhost:3333

## Pipeline

- **synthetic_support_tickets**: Generate 10M tickets (50 partitions × 200k rows)
- **ticket_embeddings**: Convert text to 384-dim vectors using SentenceTransformer
- **trained_clustering_model**: Train MiniBatchKMeans with 20 clusters
- **ticket_clusters**: Assign cluster IDs to tickets
- **cluster_analysis**: Generate business insights

## Configuration

Worker limits configured in `dagster/dagster.yaml`:
- Max 2 concurrent runs
- 1 asset per group at a time
- Reduced worker threads (2 instead of 8)
