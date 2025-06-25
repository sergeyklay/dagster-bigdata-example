from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
    """Memory-efficient IO Manager using PyArrow for Parquet files in S3."""

    s3_resource: S3Resource
    s3_bucket: str

    def _get_path(self, context) -> str:
        """Generate S3 path for the asset."""
        asset_name = "/".join(context.asset_key.path)
        if context.has_asset_partitions:
            return f"{asset_name}/{context.asset_partition_key}.parquet"
        return f"{asset_name}.parquet"

    def handle_output(self, context, obj: pd.DataFrame | pa.Table | str | dict) -> None:
        """Store DataFrame, PyArrow Table, or other objects in S3."""
        key = self._get_path(context)
        context.log.info("Storing data to s3://%s/%s", self.s3_bucket, key)

        try:
            # Handle different object types
            if isinstance(obj, (pd.DataFrame, pa.Table)):
                # Convert pandas DataFrame to PyArrow Table for better memory efficiency
                if isinstance(obj, pd.DataFrame):
                    # Use PyArrow for more efficient memory usage
                    table = pa.Table.from_pandas(obj, preserve_index=False)
                else:
                    table = obj

                # Use memory-efficient buffer streaming
                buffer = BytesIO()

                # Write with optimized Parquet settings for large datasets
                pq.write_table(
                    table,
                    buffer,
                    # Memory optimization settings
                    compression="snappy",  # Good compression/speed balance
                    use_dictionary=True,  # Reduce memory for repeated values
                    write_statistics=True,  # Enable predicate pushdown
                    row_group_size=50000,  # Smaller row groups for streaming
                    data_page_size=1024 * 1024,  # 1MB pages for efficient I/O
                )

                # Get buffer content
                buffer.seek(0)
                parquet_bytes = buffer.getvalue()

                # Upload to S3
                self.s3_resource.get_client().put_object(
                    Bucket=self.s3_bucket,
                    Key=key,
                    Body=parquet_bytes,
                    ContentType="application/octet-stream",
                )

                # Log memory usage info
                context.log.info(
                    "Stored %d bytes to s3://%s/%s",
                    len(parquet_bytes),
                    self.s3_bucket,
                    key,
                )
                context.log.info(
                    "PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20
                )

            elif isinstance(obj, str):
                # Store string (like file paths)
                self.s3_resource.get_client().put_object(
                    Bucket=self.s3_bucket,
                    Key=key.replace(".parquet", ".txt"),
                    Body=obj.encode("utf-8"),
                    ContentType="text/plain",
                )
                context.log.info("Stored string to s3://%s/%s", self.s3_bucket, key)

            elif isinstance(obj, dict):
                # Store dict as JSON
                import json

                json_content = json.dumps(obj, indent=2, default=str)
                self.s3_resource.get_client().put_object(
                    Bucket=self.s3_bucket,
                    Key=key.replace(".parquet", ".json"),
                    Body=json_content.encode("utf-8"),
                    ContentType="application/json",
                )
                context.log.info("Stored dict to s3://%s/%s", self.s3_bucket, key)

        except Exception as e:
            context.log.error("Error storing data to S3: %s", str(e))
            raise

    def load_input(self, context) -> pd.DataFrame | str | dict:
        """Load data from S3 with memory optimization."""
        key = self._get_path(context)
        context.log.info("Loading data from s3://%s/%s", self.s3_bucket, key)

        try:
            # Try to load as Parquet first
            response = self.s3_resource.get_client().get_object(
                Bucket=self.s3_bucket, Key=key
            )

            # Read streaming response efficiently
            parquet_bytes = response["Body"].read()

            # Use PyArrow for memory-efficient reading
            buffer = BytesIO(parquet_bytes)

            # Read with PyArrow for better memory management
            table = pq.read_table(
                buffer,
                # Memory optimization settings
                use_threads=True,  # Enable multi-threading
                buffer_size=1024 * 1024,  # 1MB buffer for streaming
                pre_buffer=True,  # Pre-buffer for better performance
            )

            # Convert to pandas with memory optimization
            df = table.to_pandas(
                # Memory optimization settings
                self_destruct=True,  # Free Arrow memory after conversion
                ignore_metadata=True,  # Skip unnecessary metadata
                types_mapper=pd.ArrowDtype,  # Use Arrow-backed pandas types
            )

            context.log.info(
                "Loaded %d rows from s3://%s/%s", len(df), self.s3_bucket, key
            )
            context.log.info(
                "PyArrow memory allocated: %dMB", pa.total_allocated_bytes() >> 20
            )

            return df

        except Exception as e:
            context.log.warning("Failed to load as parquet from %s: %s", key, str(e))

            # If not Parquet, try as text
            try:
                response = self.s3_resource.get_client().get_object(
                    Bucket=self.s3_bucket, Key=key.replace(".parquet", ".txt")
                )
                content = response["Body"].read().decode("utf-8")
                context.log.info("Loaded string from s3://%s/%s", self.s3_bucket, key)
                return content
            except Exception as e2:
                context.log.warning("Failed to load as text: %s", str(e2))

                # Try as JSON
                try:
                    response = self.s3_resource.get_client().get_object(
                        Bucket=self.s3_bucket, Key=key.replace(".parquet", ".json")
                    )
                    import json

                    content = json.loads(response["Body"].read().decode("utf-8"))
                    context.log.info("Loaded dict from s3://%s/%s", self.s3_bucket, key)
                    return content
                except Exception as e3:
                    context.log.error(
                        "Failed to load from %s in any format: %s", key, str(e3)
                    )
                    raise


def create_s3_io_manager():
    """Create S3 IO manager with Parquet support."""
    return S3ParquetIOManager(
        s3_resource=create_s3_resource(),
        s3_bucket="dbe",
    )
