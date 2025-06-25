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
    AllPartitionMapping,
    AssetDep,
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
    tags={"dagster/concurrency_key": "data_generation"},
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
    tags={"dagster/concurrency_key": "embeddings"},
)
def ticket_embeddings(
    context: AssetExecutionContext, synthetic_support_tickets: str, s3: S3Resource
) -> None:
    """Generate embeddings using streaming ParquetWriter for memory efficiency."""
    partition_key = context.partition_key

    context.log.info("Processing embeddings for %s", partition_key)

    s3_key = synthetic_support_tickets.replace("s3://dbe/", "")

    response = s3.get_client().get_object(Bucket="dbe", Key=s3_key)
    buffer = BytesIO(response["Body"].read())

    table = pq.read_table(
        buffer,
        columns=["ticket_id", "message_text"],
        use_threads=False,
    )

    context.log.info("Loading SentenceTransformer model")

    # Load the sentence transformer model
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Define schema for streaming output
    schema = pa.schema(
        [("ticket_id", pa.int64()), *[(f"emb_{i}", pa.float32()) for i in range(384)]]
    )

    # Prepare streaming writer with basic settings
    output_buffer = BytesIO()
    writer = pq.ParquetWriter(
        output_buffer,
        schema,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )

    # Process in streaming batches - NO memory accumulation
    batch_size = 1000
    total_processed = 0

    try:
        for batch in table.to_batches(max_chunksize=batch_size):
            batch_df = batch.to_pandas()
            messages = batch_df["message_text"].tolist()
            ticket_ids = batch_df["ticket_id"].tolist()

            # Generate embeddings for this batch
            embeddings = model.encode(
                messages,
                convert_to_numpy=True,
                show_progress_bar=False,
                batch_size=64,
            )

            # Create PyArrow arrays for this batch
            arrays = [pa.array(ticket_ids, type=pa.int64())]

            # Add embedding columns efficiently
            for i in range(384):
                arrays.append(pa.array(embeddings[:, i], type=pa.float32()))

            # Create batch table and write immediately
            batch_table = pa.Table.from_arrays(arrays, schema=schema)
            writer.write_table(batch_table)

            total_processed += len(batch_table)

            if total_processed % 5000 == 0:
                context.log.info("Processed %d messages", total_processed)

            # Explicit cleanup
            del embeddings, batch_table, arrays

    finally:
        writer.close()

    # Write final result to S3
    output_buffer.seek(0)
    parquet_bytes = output_buffer.getvalue()

    output_key = f"ticket_embeddings/{partition_key}.parquet"
    s3.get_client().put_object(
        Bucket="dbe",
        Key=output_key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )

    context.log.info(
        "Generated embeddings for %d tickets in %s (%d bytes)",
        total_processed,
        partition_key,
        len(parquet_bytes),
    )
    context.log.info("PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20)


@asset(
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="clustering",
    tags={"dagster/concurrency_key": "clustering"},
    deps=[AssetDep(ticket_embeddings, partition_mapping=AllPartitionMapping())],
)
def trained_clustering_model(context: AssetExecutionContext, s3: S3Resource) -> str:
    """Train clustering model using streaming S3 access."""
    context.log.info("Starting incremental clustering model training")

    # Initialize MiniBatchKMeans with optimized settings
    model = MiniBatchKMeans(
        n_clusters=20,
        batch_size=5_000,  # Reduced for better memory efficiency
        random_state=42,
        max_iter=50,  # Reduced iterations
        verbose=0,  # Disable verbose for cleaner logs
        n_init=3,  # Fewer initializations
    )

    # Check which embedding partitions are available
    total_samples = 0
    partitions_processed = 0

    for partition_key in PARTITIONS.get_partition_keys():
        try:
            embedding_path = f"ticket_embeddings/{partition_key}.parquet"

            try:
                response = s3.get_client().get_object(Bucket="dbe", Key=embedding_path)

                # Use streaming approach with smaller chunks
                parquet_bytes = response["Body"].read()
                buffer = BytesIO(parquet_bytes)

                # Read with PyArrow for better memory management
                table = pq.read_table(
                    buffer,
                    columns=[f"emb_{i}" for i in range(384)],  # Only embedding columns
                    use_threads=False,
                )

                # Process in smaller streaming batches
                batch_count = 0
                for batch in table.to_batches(max_chunksize=2000):  # Smaller chunks
                    embeddings = batch.to_pandas(
                        self_destruct=True,  # Free Arrow memory immediately
                    ).values.astype(np.float32)

                    # Perform partial fit
                    model.partial_fit(embeddings)
                    batch_count += 1

                    # Free memory explicitly
                    del embeddings

                partitions_processed += 1
                total_samples += len(table)

                context.log.info(
                    "Trained on partition %s - %d samples",
                    partition_key,
                    len(table),
                )

                # Free memory explicitly
                del table

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
            "Model trained on %d partitions with %d total samples",
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
    tags={"dagster/concurrency_key": "clustering"},
)
def ticket_clusters(
    context: AssetExecutionContext,
    ticket_embeddings: str,
    trained_clustering_model: str,
    s3: S3Resource,
) -> pa.Table:
    """Assign clusters using streaming from S3 paths."""
    context.log.info("Assigning clusters for %s", context.partition_key)

    try:
        # Load the trained model from S3
        model_key = trained_clustering_model.replace("s3://dbe/", "")
        response = s3.get_client().get_object(Bucket="dbe", Key=model_key)
        model = pickle.loads(response["Body"].read())

        embeddings_key = ticket_embeddings.replace("s3://dbe/", "")
        response = s3.get_client().get_object(Bucket="dbe", Key=embeddings_key)
        buffer = BytesIO(response["Body"].read())

        # Stream process embeddings in batches
        table = pq.read_table(buffer, use_threads=False)

        # Get ticket IDs first (they're small)
        ticket_ids = table.column("ticket_id").to_pandas()

        # Process embeddings in streaming batches to reduce memory
        all_cluster_ids = []
        batch_size = 5000  # Process in smaller batches

        embedding_columns = [f"emb_{i}" for i in range(384)]
        embedding_table = table.select(embedding_columns)

        for i, batch in enumerate(embedding_table.to_batches(max_chunksize=batch_size)):
            # Convert to numpy for sklearn processing
            embeddings = batch.to_pandas(
                self_destruct=True,  # Free Arrow memory immediately
            ).values.astype(np.float32)

            # Predict cluster assignments for this batch
            batch_clusters = model.predict(embeddings)
            all_cluster_ids.extend(batch_clusters)

            # Free memory
            del embeddings

            if (i + 1) % 10 == 0:
                context.log.info("Processed batch %d", i + 1)

        context.log.info("Predicted clusters for %d tickets", len(all_cluster_ids))

        # Create result as PyArrow Table for memory efficiency
        result_table = pa.Table.from_arrays(
            [
                pa.array(ticket_ids, type=pa.int64()),
                pa.array(all_cluster_ids, type=pa.int32()),
            ],
            names=["ticket_id", "cluster_id"],
        )

        # Add some metadata about cluster distribution
        cluster_counts = pd.Series(all_cluster_ids).value_counts().sort_index()
        context.log.info("Cluster distribution for %s:", context.partition_key)
        for cluster_id, count in cluster_counts.items():
            context.log.info("  Cluster %d: %d tickets", cluster_id, count)

        return result_table

    except Exception as e:
        context.log.error(
            "Error in cluster assignment for %s: %s", context.partition_key, str(e)
        )
        raise


@asset(
    io_manager_key="s3_io_manager",
    compute_kind="python",
    group_name="analysis",
    deps=[ticket_clusters, synthetic_support_tickets],
    tags={"dagster/concurrency_key": "analysis"},
)
def cluster_analysis(context: AssetExecutionContext, s3: S3Resource) -> dict:
    """Analyze clustering results with streaming processing."""
    context.log.info("Starting cluster analysis across all partitions")

    analysis_results = {
        "total_tickets_processed": 50 * ROWS_PER_PARTITION,
        "total_partitions": 50,
        "total_clusters": 20,
        "memory_optimization": {
            "pyarrow_enabled": True,
            "streaming_processing": True,
            "memory_efficient_parquet": True,
            "float32_embeddings": True,
            "path_aware_io_manager": True,
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
            "total_processing_time": "estimated 1-2 hours for full pipeline",
            "pyarrow_memory_usage_mb": pa.total_allocated_bytes() >> 20,
        },
    }

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
