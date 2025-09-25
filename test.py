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
