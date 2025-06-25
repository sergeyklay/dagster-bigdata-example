import dagster as dg

from .assets import (
    cluster_analysis,
    synthetic_support_tickets,
    ticket_clusters,
    ticket_embeddings,
    trained_clustering_model,
)
from .resources import create_s3_io_manager, create_s3_resource

defs = dg.Definitions(
    assets=[
        synthetic_support_tickets,
        ticket_embeddings,
        trained_clustering_model,
        ticket_clusters,
        cluster_analysis,
    ],
    resources={
        "s3_io_manager": create_s3_io_manager(),
        "s3": create_s3_resource(),
    },
)
