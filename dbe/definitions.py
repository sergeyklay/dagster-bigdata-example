import dagster as dg

from .assets import processed_data, processed_data_csv, sample_data
from .resources import create_s3_io_manager, create_s3_resource

defs = dg.Definitions(
    assets=[sample_data, processed_data, processed_data_csv],
    resources={
        "s3_io_manager": create_s3_io_manager(),
        "s3": create_s3_resource(),
    },
)
