import pandas as pd
from dagster_aws.s3 import S3PickleIOManager, S3Resource

import dagster as dg


def create_s3_resource():
    """Create S3 resource for Minio."""
    return S3Resource(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="miniosecret",
        region_name="us-east-1",
    )


def create_s3_io_manager():
    """Create S3 IO Manager."""
    return S3PickleIOManager(
        s3_resource=create_s3_resource(),
        s3_bucket="dbe",
    )


@dg.asset(io_manager_key="s3_io_manager")
def sample_data() -> pd.DataFrame:
    """Load the sample data from local CSV file."""
    df = pd.read_csv("data/sample_data.csv")
    return df


@dg.asset(io_manager_key="s3_io_manager")
def processed_data(sample_data: pd.DataFrame) -> pd.DataFrame:
    """Process the sample data and add age group classification."""
    df = sample_data.copy()
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    return df


@dg.asset(io_manager_key="s3_io_manager")
def processed_data_csv(
    context: dg.AssetExecutionContext, processed_data: pd.DataFrame, s3: S3Resource
) -> str:
    """Save processed data as CSV to S3 bucket."""
    csv_content = processed_data.to_csv(index=False)

    s3_key = "processed_data/processed_data.csv"

    s3.get_client().put_object(
        Bucket="dbe",
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )

    context.log.info("Uploaded processed data CSV to s3://dbe/%s", s3_key)
    return f"s3://dbe/{s3_key}"


defs = dg.Definitions(
    assets=[sample_data, processed_data, processed_data_csv],
    resources={
        "s3_io_manager": create_s3_io_manager(),
        "s3": create_s3_resource(),
    },
)
