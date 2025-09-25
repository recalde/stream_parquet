# lambda_handler.py
import os
import time
import logging
from typing import List, Dict, Any

import boto3
import pandas as pd
import awswrangler as wr

from parquet_helpers import (
    LOG_TAG,
    human_readable_bytes,
    human_readable_rows,
    build_s3_key_with_local_hour,
    get_batch_event_time_utc,
    get_table_name_from_stream_arn,
    deserialize_stream_new_image,
    coerce_dataframe_types_with_schema,
)
from schema_cache import ensure_schema_cache_loaded, get_table_schema_or_raise

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Entrypoint; writes strongly typed Parquet for a DDB Streams batch."""
    start_wall = time.time()

    # Pre-read table for start log
    records: List[Dict[str, Any]] = event.get("Records", []) or []
    first_table = get_table_name_from_stream_arn(records[0].get("eventSourceARN", "")) if records else "unknown"

    # Friendly + searchable START line
    logger.info("🌟 %s | start table=%s request_id=%s",
                LOG_TAG, first_table, getattr(context, "aws_request_id", "n/a"))

    # Required env
    bucket = os.environ.get("BUCKET_NAME")
    base_prefix = os.environ.get("BASE_PREFIX")
    if not bucket or not base_prefix:
        raise RuntimeError("❌ BUCKET_NAME and BASE_PREFIX env vars are required.")

    # Load schema.csv (cached across warm starts)
    ensure_schema_cache_loaded()

    if not records:
        duration = time.time() - start_wall
        logger.info("ℹ️ %s | empty_batch table=%s", LOG_TAG, first_table)
        logger.info("🛑 %s | finish table=%s duration_s=%.2f rows=0 size=0B skipped=0 status=empty",
                    LOG_TAG, first_table, duration)
        return {"status": "empty", "written": 0}

    table = first_table
    schema_df = get_table_schema_or_raise(table)  # raises if missing

    # Collect INSERT/MODIFY items
    items, skipped = [], 0
    for r in records:
        if r.get("eventName") in ("INSERT", "MODIFY"):
            obj = deserialize_stream_new_image(r)
            if obj:
                items.append(obj)
            else:
                skipped += 1
        else:
            skipped += 1

    if not items:
        duration = time.time() - start_wall
        logger.info("ℹ️ %s | no_op table=%s skipped=%d", LOG_TAG, table, skipped)
        logger.info("🛑 %s | finish table=%s duration_s=%.2f rows=0 size=0B skipped=%d status=no-op",
                    LOG_TAG, table, duration, skipped)
        return {"status": "no-op", "skipped": skipped, "table": table}

    df = pd.DataFrame(items)
    df.columns = df.columns.str.strip()  # normalize headers

    # Strong typing + column order per schema
    df, dtype_map = coerce_dataframe_types_with_schema(df, schema_df)

    # S3 key uses the New York hour (or env TIMEZONE)
    event_dt_utc = get_batch_event_time_utc(records)
    s3_key = build_s3_key_with_local_hour(base_prefix, table, event_dt_utc)
    s3_uri = f"s3://{bucket}/{s3_key}"

    logger.info("🚀 %s | write_start table=%s rows=%d path=%s",
                LOG_TAG, table, len(df), s3_uri)

    # Write one Parquet file per batch, strongly typed (pyarrow via awswrangler)
    result = wr.s3.to_parquet(
        df=df,
        path=s3_uri,
        dtype=dtype_map,
        index=False,
        dataset=False,
        compression="snappy",
    )

    # Get object size for pretty summary
    size_bytes = None
    try:
        s3 = boto3.client("s3")
        head = s3.head_object(Bucket=bucket, Key=s3_key)
        size_bytes = head.get("ContentLength")
    except Exception:
        pass

    logger.info("✅ %s | write_done table=%s rows=%d size=%s size_bytes=%s s3_key=%s skipped=%d",
                LOG_TAG, table, len(df),
                human_readable_bytes(size_bytes) if size_bytes else "size n/a",
                size_bytes if size_bytes is not None else "n/a",
                s3_key, skipped)

    duration = time.time() - start_wall
    logger.info("🛑 %s | finish table=%s duration_s=%.2f rows=%d size=%s size_bytes=%s skipped=%d status=ok",
                LOG_TAG, table, duration, len(df),
                human_readable_bytes(size_bytes) if size_bytes else "size n/a",
                size_bytes if size_bytes is not None else "n/a",
                skipped)

    return {
        "status": "ok",
        "table": table,
        "rows": len(df),
        "skipped": skipped,
        "s3_key": s3_key,
        "s3_uri": s3_uri,
        "size_bytes": size_bytes,
        "duration_seconds": round(duration, 2),
        "paths": result if isinstance(result, list) else [result],
    }
