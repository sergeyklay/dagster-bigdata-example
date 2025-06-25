from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_aws.s3 import S3Resource

from dagster import (
    ConfigurableIOManager,
)


def create_s3_resource():
    """Create S3 resource for Minio."""
    return S3Resource(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="miniosecret",
        region_name="us-east-1",
    )


class S3PathAwareIOManager(ConfigurableIOManager):
    """PathAware IO Manager that returns S3 paths instead of loading data."""

    s3_resource: S3Resource
    s3_bucket: str

    def _get_path(self, context) -> str:
        """Generate S3 path for the asset."""
        asset_name = "/".join(context.asset_key.path)
        if context.has_asset_partitions:
            return f"{asset_name}/{context.asset_partition_key}.parquet"
        return f"{asset_name}.parquet"

    def handle_output(self, context, obj) -> None:
        """Store data to S3."""
        key = self._get_path(context)
        s3_path = f"s3://{self.s3_bucket}/{key}"

        if isinstance(obj, (pd.DataFrame, pa.Table)):
            if isinstance(obj, pd.DataFrame):
                table = pa.Table.from_pandas(obj, preserve_index=False)
            else:
                table = obj

            buffer = BytesIO()
            pq.write_table(
                table,
                buffer,
                compression="snappy",
                use_dictionary=True,
                write_statistics=True,
                row_group_size=50000,
                data_page_size=1024 * 1024,
            )

            buffer.seek(0)
            parquet_bytes = buffer.getvalue()

            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
            )

            context.log.info(
                "Stored %d rows to %s (%d bytes)",
                len(table),
                s3_path,
                len(parquet_bytes),
            )

        elif isinstance(obj, str):
            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=key.replace(".parquet", ".txt"),
                Body=obj.encode("utf-8"),
                ContentType="text/plain",
            )
            context.log.info("Stored string to %s", s3_path)

        elif isinstance(obj, dict):
            import json

            json_content = json.dumps(obj, indent=2, default=str)
            self.s3_resource.get_client().put_object(
                Bucket=self.s3_bucket,
                Key=key.replace(".parquet", ".json"),
                Body=json_content.encode("utf-8"),
                ContentType="application/json",
            )
            context.log.info("Stored dict to %s", s3_path)

    def load_input(self, context) -> str:
        """Return S3 path instead of loading data."""
        key = self._get_path(context)
        s3_path = f"s3://{self.s3_bucket}/{key}"
        context.log.info("Returning S3 path: %s", s3_path)
        return s3_path


def create_s3_io_manager():
    """Create PathAware S3 IO manager."""
    return S3PathAwareIOManager(
        s3_resource=create_s3_resource(),
        s3_bucket="dbe",
    )
