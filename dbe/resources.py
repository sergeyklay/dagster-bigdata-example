from io import BytesIO

import pandas as pd
from dagster_aws.s3 import S3Resource

from dagster import ConfigurableIOManager


def create_s3_resource():
    """Create S3 resource for Minio."""
    return S3Resource(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="miniosecret",
        region_name="us-east-1",
    )


class S3ParquetIOManager(ConfigurableIOManager):
    """IO Manager that stores Pandas DataFrames as Parquet files in S3."""

    s3_resource: S3Resource
    s3_bucket: str

    def _get_path(self, context) -> str:
        """Generate S3 path for the asset."""
        asset_name = "/".join(context.asset_key.path)
        if context.has_asset_partitions:
            return f"{asset_name}/{context.asset_partition_key}.parquet"
        return f"{asset_name}.parquet"

    def handle_output(self, context, obj: pd.DataFrame | str | dict):
        """Store the asset output in S3."""
        s3_path = self._get_path(context)

        if isinstance(obj, pd.DataFrame):
            # Store DataFrame as Parquet using PyArrow
            buffer = BytesIO()
            obj.to_parquet(buffer, index=False)
            buffer.seek(0)

            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=s3_path,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            context.log.info(
                "Stored DataFrame with %d rows to s3://%s/%s",
                len(obj),
                self.s3_bucket,
                s3_path,
            )
        elif isinstance(obj, str):
            # Store string (like file paths)
            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=s3_path.replace(".parquet", ".txt"),
                Body=obj.encode("utf-8"),
                ContentType="text/plain",
            )
            context.log.info("Stored string to s3://%s/%s", self.s3_bucket, s3_path)
        elif isinstance(obj, dict):
            # Store dict as JSON
            import json

            json_content = json.dumps(obj, indent=2, default=str)
            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=s3_path.replace(".parquet", ".json"),
                Body=json_content.encode("utf-8"),
                ContentType="application/json",
            )
            context.log.info("Stored dict to s3://%s/%s", self.s3_bucket, s3_path)

    def load_input(self, context) -> pd.DataFrame | str | dict:
        """Load the asset input from S3."""
        s3_path = self._get_path(context)

        try:
            # Try to load as Parquet first
            response = self.s3_resource.get_client().get_object(
                Bucket=self.s3_bucket, Key=s3_path
            )

            # Read the streaming response body into bytes first
            parquet_bytes = response["Body"].read()

            # Use BytesIO to create a file-like object that pandas can read
            parquet_buffer = BytesIO(parquet_bytes)
            df = pd.read_parquet(parquet_buffer)

            context.log.info(
                "Loaded DataFrame with %d rows from s3://%s/%s",
                len(df),
                self.s3_bucket,
                s3_path,
            )
            return df

        except Exception as e:
            context.log.warning(
                "Failed to load as parquet from %s: %s", s3_path, str(e)
            )

            # If not Parquet, try as text
            try:
                response = self.s3_resource.get_client().get_object(
                    Bucket=self.s3_bucket, Key=s3_path.replace(".parquet", ".txt")
                )
                content = response["Body"].read().decode("utf-8")
                context.log.info(
                    "Loaded string from s3://%s/%s", self.s3_bucket, s3_path
                )
                return content
            except Exception as e2:
                context.log.warning("Failed to load as text: %s", str(e2))

                # Try as JSON
                try:
                    response = self.s3_resource.get_client().get_object(
                        Bucket=self.s3_bucket, Key=s3_path.replace(".parquet", ".json")
                    )
                    import json

                    content = json.loads(response["Body"].read().decode("utf-8"))
                    context.log.info(
                        "Loaded dict from s3://%s/%s", self.s3_bucket, s3_path
                    )
                    return content
                except Exception as e3:
                    context.log.error(
                        "Failed to load from %s in any format: %s", s3_path, str(e3)
                    )
                    raise


def create_s3_io_manager():
    """Create S3 Parquet IO Manager."""
    return S3ParquetIOManager(
        s3_resource=create_s3_resource(),
        s3_bucket="dbe",
    )
