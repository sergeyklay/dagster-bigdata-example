import pickle

import numpy as np
import pandas as pd
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
def synthetic_support_tickets(context: AssetExecutionContext) -> pd.DataFrame:
    """Generate fake support ticket data partitioned into 50 files of 200k rows each."""
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

    df = pd.DataFrame(tickets)

    context.log.info("Generated %d tickets for %s", len(df), partition_key)
    return df


@asset(
    partitions_def=PARTITIONS,
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="embeddings",
    deps=[synthetic_support_tickets],
)
def ticket_embeddings(
    context: AssetExecutionContext, synthetic_support_tickets: pd.DataFrame
) -> pd.DataFrame:
    """Generate embeddings for support ticket messages using SentenceTransformer."""
    partition_key = context.partition_key

    context.log.info("Processing embeddings for %s", partition_key)
    context.log.info("Loading SentenceTransformer model")

    # Load the sentence transformer model
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Extract message texts
    messages = synthetic_support_tickets["message_text"].tolist()
    ticket_ids = synthetic_support_tickets["ticket_id"].tolist()

    context.log.info("Generating embeddings for %s messages", len(messages))

    # Generate embeddings in batches to manage memory
    batch_size = 128
    all_embeddings = []

    for i in range(0, len(messages), batch_size):
        batch_messages = messages[i : i + batch_size]
        batch_embeddings = model.encode(batch_messages)
        all_embeddings.extend(batch_embeddings)

        if (i // batch_size + 1) % 10 == 0:
            context.log.info("Processed %s messages", i + len(batch_messages))

    # Convert to numpy array
    embeddings_array = np.array(all_embeddings)

    # Create DataFrame with ticket_id and embedding dimensions
    embedding_data = {"ticket_id": ticket_ids}
    for i in range(embeddings_array.shape[1]):
        embedding_data[f"emb_{i}"] = embeddings_array[:, i].tolist()

    df = pd.DataFrame(embedding_data)

    context.log.info(
        "Generated embeddings with shape %s for %s",
        embeddings_array.shape,
        partition_key,
    )

    return df


@asset(
    io_manager_key="s3_io_manager",
    compute_kind="semanticmodel",
    group_name="clustering",
)
def trained_clustering_model(context: AssetExecutionContext, s3: S3Resource) -> str:
    """Train model using incremental learning on available embedding partitions."""
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
            # Try to load embeddings for this partition
            embedding_path = f"ticket_embeddings/{partition_key}.parquet"

            try:
                response = s3.get_client().get_object(Bucket="dbe", Key=embedding_path)

                # Read the streaming response body into bytes first
                parquet_bytes = response["Body"].read()

                # Use BytesIO to create a file-like object that pandas can read
                from io import BytesIO

                parquet_buffer = BytesIO(parquet_bytes)
                df = pd.read_parquet(parquet_buffer)

                # Extract embedding features (exclude ticket_id)
                embedding_columns = [
                    col for col in df.columns if col.startswith("emb_")
                ]
                embeddings = df[embedding_columns].values

                # Perform partial fit
                model.partial_fit(embeddings)

                partitions_processed += 1
                total_samples += len(embeddings)

                context.log.info(
                    "Trained on partition %s - %s samples",
                    partition_key,
                    len(embeddings),
                )

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
) -> pd.DataFrame:
    """Assign cluster IDs to tickets using the trained clustering model."""
    partition_key = context.partition_key

    context.log.info("Assigning clusters for %s", partition_key)

    try:
        # Load the trained model from S3
        model_key = trained_clustering_model.replace("s3://dbe/", "")
        response = s3.get_client().get_object(Bucket="dbe", Key=model_key)
        model = pickle.loads(response["Body"].read())

        # Extract embeddings (exclude ticket_id column)
        embedding_columns = [
            col for col in ticket_embeddings.columns if col.startswith("emb_")
        ]
        embeddings = ticket_embeddings[embedding_columns].values

        context.log.info("Predicting clusters for %s tickets", len(embeddings))

        # Predict cluster assignments
        cluster_ids = model.predict(embeddings)

        # Create result DataFrame
        result_df = pd.DataFrame(
            {
                "ticket_id": ticket_embeddings["ticket_id"],
                "cluster_id": cluster_ids,
            }
        )

        # Add some metadata about cluster distribution
        cluster_counts = pd.Series(cluster_ids).value_counts().sort_index()
        context.log.info("Cluster distribution for %s:", partition_key)
        for cluster_id, count in cluster_counts.items():
            context.log.info("  Cluster %s: %s tickets", cluster_id, count)

        return result_df

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
    """Analyze clustering results across all partitions to provide business insights."""
    context.log.info("Starting cluster analysis across all partitions")

    # Fake analysis structure
    analysis_results = {
        "total_tickets_processed": 50 * ROWS_PER_PARTITION,
        "total_partitions": 50,
        "total_clusters": 20,
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

        return analysis_results

    except Exception as e:
        context.log.error("Error saving analysis: %s", str(e))
        raise
