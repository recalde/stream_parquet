# test.py
"""
Local integration tester that:
- Finds all DynamoDB tables with a given prefix.
- Reads up to 1000 items from each via Scan.
- Wraps them as a fake DynamoDB Streams event.
- Invokes lambda_handler.lambda_handler(event, context) for each table.
Notes:
- This uses your real AWS credentials in the environment.
- Set env BUCKET_NAME/BASE_PREFIX/TIMEZONE for the handler output.
- Set env TABLE_PREFIX (defaults to "dev_tables") to choose which tables to test.
"""
import os
import time
import json
import random
import logging
from types import SimpleNamespace
from typing import Dict, Any, List

import boto3
from boto3.dynamodb.types import TypeSerializer

from lambda_handler import lambda_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_runner")


def build_fake_stream_event(region: str, account_id: str, table_name: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a minimal DDB Streams batch with INSERT events for provided items."""
    ts = TypeSerializer()
    now = time.time()
    arn = f"arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/stream/{int(now)}"
    records = []
    for it in items:
        # Convert Python dict → AttributeValue map for NewImage
        new_image = ts.serialize(it)["M"]
        records.append({
            "eventID": f"{table_name}-{random.randint(1, 1_000_000)}",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": region,
            "eventSourceARN": arn,
            "dynamodb": {
                "ApproximateCreationDateTime": now,
                "NewImage": new_image,
                # Keys/OldImage omitted for brevity
            },
        })
    return {"Records": records}


def main():
    # Configure test scope and env
    table_prefix = os.environ.get("TABLE_PREFIX", "dev_tables")
    region = boto3.session.Session().region_name or "us-east-1"
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]

    ddb = boto3.client("dynamodb", region_name=region)

    # Local-output stubbing: if LOCAL_OUTPUT is "1" (default) we stub awswrangler -> write to local ./output/
    if os.environ.get("LOCAL_OUTPUT", "1") == "1":
        from pathlib import Path
        import awswrangler as wr
        import pandas as pd

        base_dir = Path(__file__).parent
        output_dir = Path(os.environ.get("LOCAL_OUTPUT_DIR", str(base_dir / "output")))
        output_dir.mkdir(parents=True, exist_ok=True)

        def _stub_to_parquet(df: pd.DataFrame, path: str, dtype=None, index=False, dataset=False, compression=None, **kwargs):
            """
            Convert s3://bucket/key... into a local path under output_dir/<key> and write a parquet file.
            Returns the local path string (mimicking wr.s3.to_parquet).
            """
            bucket = os.environ.get("BUCKET_NAME", "")
            if path.startswith(f"s3://{bucket}/"):
                key = path.split(f"s3://{bucket}/", 1)[1]
            elif path.startswith("s3://"):
                key = path.split("s3://", 1)[1]
            else:
                key = path

            local_path = output_dir / key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                # prefer parquet; if pyarrow/fastparquet not available, fall back to feather
                df.to_parquet(local_path, index=index)
            except Exception:
                local_path = local_path.with_suffix(".feather")
                df.reset_index(drop=True).to_feather(local_path)
            # Return a path-like string similar to awswrangler (we return local path)
            return str(local_path)

        # Patch the awswrangler to_parquet used by lambda_handler
        try:
            wr.s3.to_parquet = _stub_to_parquet
        except Exception:
            # best-effort patch; if wr module shape differs, ignore to avoid breaking real runs
            pass

        # Patch boto3.client to return a stub S3 client for head_object requests
        real_boto3_client = boto3.client

        class _StubS3Client:
            def head_object(self, Bucket, Key):
                # Map s3 Key to local output path
                local_candidate = output_dir / Key
                if not local_candidate.exists():
                    # attempt to find any file under output_dir as a fallback
                    found = next(output_dir.rglob("*"), None)
                    local_candidate = found if found and found.exists() else None
                size = local_candidate.stat().st_size if local_candidate and local_candidate.exists() else 0
                return {"ContentLength": int(size)}

        def _client(name, *args, **kwargs):
            if name == "s3":
                return _StubS3Client()
            return real_boto3_client(name, *args, **kwargs)

        boto3.client = _client

    # List tables with prefix
    table_names = []
    paginator = ddb.get_paginator("list_tables")
    for page in paginator.paginate():
        for name in page.get("TableNames", []):
            if name.startswith(table_prefix):
                table_names.append(name)

    logger.info("Found %d table(s) with prefix '%s'", len(table_names), table_prefix)
    if not table_names:
        return

    # Scan, build fake events, invoke handler per table
    for table in sorted(table_names):
        logger.info("🔎 Testing table=%s", table)
        # Gather up to 1000 items via Scan
        scan_items: List[Dict[str, Any]] = []
        paginator = ddb.get_paginator("scan")
        for page in paginator.paginate(TableName=table, PaginationConfig={"MaxItems": 1000, "PageSize": 100}):
            scan_items.extend([ {k: boto3.dynamodb.types.TypeDeserializer().deserialize(v) for k, v in item.items()} for item in page.get("Items", []) ])
            if len(scan_items) >= 1000:
                scan_items = scan_items[:1000]
                break

        if not scan_items:
            logger.info("⚠️ No data in table=%s; skipping.", table)
            continue

        event = build_fake_stream_event(region, account_id, table, scan_items)

        # Minimal context with request_id
        context = SimpleNamespace(aws_request_id=f"test-{table}-{int(time.time())}")

        # Invoke handler and print concise result
        result = lambda_handler(event, context)
        logger.info("✅ Done table=%s rows=%s size_bytes=%s s3_key=%s status=%s",
                    table, result.get("rows"), result.get("size_bytes"),
                    result.get("s3_key"), result.get("status"))


if __name__ == "__main__":
    main()
