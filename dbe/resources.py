from dagster_aws.s3 import S3PickleIOManager, S3Resource


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
