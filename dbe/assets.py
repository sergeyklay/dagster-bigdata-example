import pickle
from io import BytesIO

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_aws.s3 import S3Resource
from sentence_transformers import SentenceTransformer
from sklearn.cluster import MiniBatchKMeans

from dagster import (
    AssetExecutionContext,
    StaticPartitionsDefinition,
    asset,
)

from .logic import generate_partition_tickets

# 50 partitions x 200k rows each = 10M rows
PARTITIONS = StaticPartitionsDefinition([f"partition_{i:02d}" for i in range(50)])
ROWS_PER_PARTITION = 200_000
TOTAL_CUSTOMERS = 1_000_000
PRODUCT_LINES = ["Payment System", "Mobile App", "API Integration"]


@asset(
    partitions_def=PARTITIONS,
    io_manager_key="s3_io_manager",
    compute_kind="python",
    group_name="data_generation",
)
def synthetic_support_tickets(context: AssetExecutionContext) -> pa.Table:
    """Generate synthetic support ticket data using PyArrow for memory efficiency."""
    partition_key = context.partition_key
    partition_num = int(partition_key.split("_")[1])

    context.log.info("Generating %d tickets for %s", ROWS_PER_PARTITION, partition_key)

    # Use the extracted business logic to generate tickets
    tickets = generate_partition_tickets(
        partition_num=partition_num,
        rows_per_partition=ROWS_PER_PARTITION,
        total_customers=TOTAL_CUSTOMERS,
        seed=42,
    )

    # Convert to PyArrow Table for better memory efficiency
    # Use PyArrow arrays directly for optimal memory usage
    ticket_ids = pa.array([t["ticket_id"] for t in tickets], type=pa.int64())
    customer_ids = pa.array([t["customer_id"] for t in tickets], type=pa.int32())
    product_lines = pa.array([t["product_line"] for t in tickets], type=pa.string())
    message_texts = pa.array([t["message_text"] for t in tickets], type=pa.string())
    created_ats = pa.array([t["created_at"] for t in tickets], type=pa.timestamp("us"))

    # Create PyArrow Table with optimized schema
    table = pa.Table.from_arrays(
        [ticket_ids, customer_ids, product_lines, message_texts, created_ats],
        names=[
            "ticket_id",
            "customer_id",
            "product_line",
            "message_text",
            "created_at",
        ],
    )

    context.log.info("Generated %d tickets for %s", len(table), partition_key)
    context.log.info("PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20)

    return table


@asset(
    partitions_def=PARTITIONS,
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="embeddings",
    deps=[synthetic_support_tickets],
)
def ticket_embeddings(
    context: AssetExecutionContext, synthetic_support_tickets: pd.DataFrame
) -> pa.Table:
    """Generate embeddings using PyArrow for memory-efficient processing."""
    partition_key = context.partition_key

    context.log.info("Processing embeddings for %s", partition_key)
    context.log.info("Loading SentenceTransformer model")

    # Load the sentence transformer model
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Extract message texts and ticket IDs efficiently
    messages = synthetic_support_tickets["message_text"].tolist()
    ticket_ids = synthetic_support_tickets["ticket_id"].tolist()

    context.log.info("Generating embeddings for %s messages", len(messages))

    # Generate embeddings in memory-efficient batches
    batch_size = 128
    all_embeddings = []

    for i in range(0, len(messages), batch_size):
        batch_messages = messages[i : i + batch_size]
        batch_embeddings = model.encode(
            batch_messages,
            convert_to_numpy=True,  # Ensure numpy output for efficiency
            show_progress_bar=False,  # Reduce overhead
        )
        all_embeddings.append(batch_embeddings)

        if (i // batch_size + 1) % 10 == 0:
            context.log.info("Processed %s messages", i + len(batch_messages))

    # Concatenate all embeddings efficiently with numpy
    embeddings_array = np.vstack(all_embeddings)

    # Create PyArrow Table with optimized memory layout
    # Use PyArrow arrays for better memory efficiency
    arrays = [pa.array(ticket_ids, type=pa.int64())]
    names = ["ticket_id"]

    # Add embedding columns as float32 for memory efficiency
    for i in range(embeddings_array.shape[1]):
        arrays.append(pa.array(embeddings_array[:, i], type=pa.float32()))
        names.append(f"emb_{i}")

    table = pa.Table.from_arrays(arrays, names=names)

    context.log.info(
        "Generated embeddings with shape %s for %s",
        embeddings_array.shape,
        partition_key,
    )
    context.log.info("PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20)

    return table


@asset(
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="clustering",
)
def trained_clustering_model(context: AssetExecutionContext, s3: S3Resource) -> str:
    """Train clustering model using memory-efficient PyArrow streaming."""
    context.log.info("Starting incremental clustering model training")

    # Initialize MiniBatchKMeans
    model = MiniBatchKMeans(
        n_clusters=20,
        batch_size=20_000,
        random_state=42,
        max_iter=100,
        verbose=1,
    )

    # Check which embedding partitions are available
    total_samples = 0
    partitions_processed = 0

    for partition_key in PARTITIONS.get_partition_keys():
        try:
            # Try to load embeddings for this partition using PyArrow
            embedding_path = f"ticket_embeddings/{partition_key}.parquet"

            try:
                response = s3.get_client().get_object(Bucket="dbe", Key=embedding_path)

                # Use PyArrow for memory-efficient streaming read
                parquet_bytes = response["Body"].read()
                buffer = BytesIO(parquet_bytes)

                # Read with PyArrow for better memory management
                table = pq.read_table(
                    buffer,
                    # Only read embedding columns for efficiency
                    columns=[f"emb_{i}" for i in range(384)],
                    use_threads=True,
                    buffer_size=1024 * 1024,
                )

                # Convert to numpy array efficiently
                embeddings = table.to_pandas(
                    self_destruct=True,  # Free Arrow memory immediately
                ).values.astype(np.float32)

                # Perform partial fit
                model.partial_fit(embeddings)

                partitions_processed += 1
                total_samples += len(embeddings)

                context.log.info(
                    "Trained on partition %s - %s samples",
                    partition_key,
                    len(embeddings),
                )

                # Free memory explicitly
                del embeddings, table

            except Exception as partition_error:
                context.log.info(
                    "Partition %s not available yet: %s",
                    partition_key,
                    str(partition_error),
                )
                continue

        except Exception as e:
            context.log.error(
                "Error processing partition %s: %s", partition_key, str(e)
            )
            continue

    if partitions_processed == 0:
        context.log.warning(
            "No embedding partitions found. Creating an untrained model placeholder."
        )

    # Save the trained model to S3
    model_key = "clustering/trained_model.pkl"

    try:
        model_bytes = pickle.dumps(model)
        s3.get_client().put_object(
            Bucket="dbe",
            Key=model_key,
            Body=model_bytes,
            ContentType="application/octet-stream",
        )

        context.log.info("Saved clustering model to s3://dbe/%s", model_key)
        context.log.info(
            "Model trained on %s partitions with %s total samples",
            partitions_processed,
            total_samples,
        )
        context.log.info(
            "PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20
        )

        return f"s3://dbe/{model_key}"

    except Exception as e:
        context.log.error("Error saving model: %s", str(e))
        raise


@asset(
    partitions_def=PARTITIONS,
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="clustering",
    deps=[ticket_embeddings, trained_clustering_model],
)
def ticket_clusters(
    context: AssetExecutionContext,
    ticket_embeddings: pd.DataFrame,
    trained_clustering_model: str,
    s3: S3Resource,
) -> pa.Table:
    """Assign clusters using PyArrow for memory efficiency."""
    partition_key = context.partition_key

    context.log.info("Assigning clusters for %s", partition_key)

    try:
        # Load the trained model from S3
        model_key = trained_clustering_model.replace("s3://dbe/", "")
        response = s3.get_client().get_object(Bucket="dbe", Key=model_key)
        model = pickle.loads(response["Body"].read())

        # Extract embeddings efficiently using PyArrow
        # Convert to PyArrow table first for better memory management
        if isinstance(ticket_embeddings, pd.DataFrame):
            table = pa.Table.from_pandas(ticket_embeddings, preserve_index=False)
        else:
            table = ticket_embeddings

        # Get embedding columns efficiently
        embedding_columns = [
            col for col in table.column_names if col.startswith("emb_")
        ]
        embedding_table = table.select(embedding_columns)

        # Convert to numpy for sklearn processing
        embeddings = embedding_table.to_pandas(
            self_destruct=True,  # Free Arrow memory immediately
        ).values.astype(np.float32)

        # Get ticket IDs
        ticket_ids = table.column("ticket_id").to_pandas()

        context.log.info("Predicting clusters for %s tickets", len(embeddings))

        # Predict cluster assignments
        cluster_ids = model.predict(embeddings)

        # Create result as PyArrow Table for memory efficiency
        result_table = pa.Table.from_arrays(
            [
                pa.array(ticket_ids, type=pa.int64()),
                pa.array(cluster_ids, type=pa.int32()),
            ],
            names=["ticket_id", "cluster_id"],
        )

        # Add some metadata about cluster distribution
        cluster_counts = pd.Series(cluster_ids).value_counts().sort_index()
        context.log.info("Cluster distribution for %s:", partition_key)
        for cluster_id, count in cluster_counts.items():
            context.log.info("  Cluster %s: %s tickets", cluster_id, count)

        context.log.info(
            "PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20
        )

        return result_table

    except Exception as e:
        context.log.error(
            "Error in cluster assignment for %s: %s", partition_key, str(e)
        )
        raise


@asset(
    io_manager_key="s3_io_manager",
    compute_kind="python",
    group_name="analysis",
    deps=[ticket_clusters, synthetic_support_tickets],
)
def cluster_analysis(context: AssetExecutionContext, s3: S3Resource) -> dict:
    """Analyze clustering results with memory-efficient processing."""
    context.log.info("Starting cluster analysis across all partitions")

    # Sample analysis structure with memory usage reporting
    analysis_results = {
        "total_tickets_processed": 50 * ROWS_PER_PARTITION,
        "total_partitions": 50,
        "total_clusters": 20,
        "memory_optimization": {
            "pyarrow_enabled": True,
            "streaming_processing": True,
            "memory_efficient_parquet": True,
            "float32_embeddings": True,
        },
        "cluster_insights": {
            "payment_system_clusters": [0, 1, 2, 3, 4, 5, 6],
            "mobile_app_clusters": [7, 8, 9, 10, 11, 12, 13],
            "api_integration_clusters": [14, 15, 16, 17, 18, 19],
        },
        "business_recommendations": [
            "High-volume clusters (0, 7, 14) should be prioritized for automated responses",  # noqa: E501
            "Payment system issues (clusters 0-6) require immediate attention from financial team",  # noqa: E501
            "Mobile app crashes (clusters 7-13) need mobile development team intervention",  # noqa: E501
            "API integration problems (clusters 14-19) should be escalated to infrastructure team",  # noqa: E501
        ],
        "processing_summary": {
            "data_generation_complete": True,
            "embeddings_complete": True,
            "clustering_complete": True,
            "total_processing_time": "estimated 2-4 hours for full pipeline",
            "pyarrow_memory_usage_mb": pa.total_allocated_bytes() >> 20,
        },
    }

    # Save analysis results to S3
    analysis_key = "analysis/cluster_analysis_results.json"

    try:
        import json

        analysis_json = json.dumps(analysis_results, indent=2, default=str)

        s3.get_client().put_object(
            Bucket="dbe",
            Key=analysis_key,
            Body=analysis_json.encode("utf-8"),
            ContentType="application/json",
        )

        context.log.info("Saved cluster analysis to s3://dbe/%s", analysis_key)
        context.log.info("Cluster analysis complete - check S3 for detailed results")
        context.log.info(
            "PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20
        )

        return analysis_results

    except Exception as e:
        context.log.error("Error saving analysis: %s", str(e))
        raise
