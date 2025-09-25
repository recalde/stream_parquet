# test_basic.py
"""
Basic tests for DynamoDB AttributeValue serialization/deserialization.
"""

import os
import csv
from decimal import Decimal
from typing import Any, Dict
from types import SimpleNamespace
from pathlib import Path

import pytest
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


def _sample_for(typ: str) -> Any:
    """Return a sample Python value for a simple type token from schema.csv."""
    if typ == "string":
        return "hello-world"
    if typ == "number":
        # use Decimal to avoid float precision surprises
        return Decimal("42")
    if typ == "boolean":
        return True
    if typ == "list":
        return ["a", Decimal("1")]
    if typ == "map":
        return {"x": "y", "n": Decimal("7")}
    if typ == "binary":
        return b"\x00\x01"
    if typ == "null":
        return None
    # default fallback
    return "unknown"


def test_schema_roundtrip():
    # locate schema.csv relative to this file
    base = os.path.dirname(__file__)
    schema_path = os.path.join(base, "schema.csv")
    assert os.path.exists(schema_path), f"schema.csv not found at {schema_path}"

    # read schema rows (expect header: name,type)
    schema = []
    with open(schema_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            schema.append({"name": row["name"].strip(), "type": row["type"].strip()})

    serializer = TypeSerializer()
    deserializer = TypeDeserializer()

    # build a test item and validate roundtrip per field
    item: Dict[str, Any] = {}
    for col in schema:
        item[col["name"]] = _sample_for(col["type"])

    # For each field, serialize then deserialize and compare values
    for k, v in item.items():
        av = serializer.serialize(v)          # AttributeValue dict like {"S": "..."} or {"N":"..."}
        py = deserializer.deserialize(av)    # back to Python value

        # Normalize expected for numeric Decimal cases: deserializer returns Decimal for numbers
        if isinstance(v, Decimal):
            assert isinstance(py, Decimal), f"field={k} expected Decimal after deserialize"
            assert py == v
        else:
            # binary comes back as bytes, lists/maps preserved, None preserved
            assert py == v, f"field={k} mismatch: {py!r} != {v!r}"


def _build_stream_event_for_table(table: str, items: Dict[str, Any], region: str = "us-east-1", account: str = "000000000000"):
    """Helper to build a minimal DDB stream event with NewImage for INSERT records."""
    ts = TypeSerializer()
    now = 1234567890
    arn = f"arn:aws:dynamodb:{region}:{account}:table/{table}/stream/{int(now)}"
    records = []
    for it in items:
        new_image = ts.serialize(it)["M"]
        records.append({
            "eventID": f"{table}-1",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": region,
            "eventSourceARN": arn,
            "dynamodb": {
                "ApproximateCreationDateTime": now,
                "NewImage": new_image,
            },
        })
    return {"Records": records}


def test_simple(monkeypatch, tmp_path):
    """
    End-to-end-ish test that stubs S3 and makes the lambda write a parquet file
    under ./output/ (project relative) instead of calling real S3.
    """
    # locate project and output dir
    base = Path(__file__).parent
    output_dir = base / "output"
    output_dir.mkdir(exist_ok=True)

    # ensure env expected by lambda_handler
    os.environ["BUCKET_NAME"] = "dummy-bucket"
    os.environ["BASE_PREFIX"] = "base/prefix"
    # optional timezone not required for this test

    # read schema and build one sample item (use same schema.csv as other tests)
    schema_path = base / "schema.csv"
    assert schema_path.exists(), "schema.csv missing"

    schema = []
    with schema_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            schema.append({
                "db_table": row["db_table"].strip(),
                "db_field_name": row["db_field_name"].strip(),
                "df_type": row["df_type"].strip(),
            })

    # Build a single item dict matching field names
    item = {}
    for col in schema:
        item[col["db_field_name"]] = _sample_for(col["df_type"])

    # Build fake stream event (one record per item)
    event = _build_stream_event_for_table(schema[0]["db_table"], [item])

    # Monkeypatch awswrangler.s3.to_parquet to write locally under ./output/
    import awswrangler as wr
    import pandas as pd

    def _stub_to_parquet(df: pd.DataFrame, path: str, dtype=None, index=False, dataset=False, compression=None, **kwargs):
        """
        Convert s3://bucket/key... into a local path under ./output/<key> and write a parquet file.
        Returns the local path (string) similar to wr.s3.to_parquet.
        """
        # path will be like s3://{bucket}/{key}
        bucket = os.environ.get("BUCKET_NAME")
        if path.startswith(f"s3://{bucket}/"):
            key = path.split(f"s3://{bucket}/", 1)[1]
        elif path.startswith("s3://"):
            # fallback strip scheme
            key = path.split("s3://", 1)[1]
        else:
            key = path

        local_path = output_dir / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        # Use pandas to_parquet; avoid requiring snappy if not available by omitting compression if needed
        try:
            df.to_parquet(local_path, index=index)
        except Exception:
            # last-resort: write Feather (if parquet fails)
            local_path = local_path.with_suffix(".feather")
            df.reset_index(drop=True).to_feather(local_path)
        return str(local_path)

    monkeypatch.setattr(wr.s3, "to_parquet", _stub_to_parquet)

    # Monkeypatch boto3.client to stub s3.head_object to report local file size
    import boto3

    real_boto3_client = boto3.client

    class _StubS3Client:
        def head_object(self, Bucket, Key):
            # Map s3 key to local output path
            bucket = Bucket
            # The lambda passes Key as the full key (without bucket)
            local_candidate = output_dir / Key
            if not local_candidate.exists():
                # sometimes the stub returned a path with s3://bucket/... split; try alternative
                local_candidate = next(output_dir.rglob("*"), None)
            size = local_candidate.stat().st_size if local_candidate and local_candidate.exists() else 0
            return {"ContentLength": int(size)}

    def _client(name, *args, **kwargs):
        if name == "s3":
            return _StubS3Client()
        return real_boto3_client(name, *args, **kwargs)

    monkeypatch.setattr(boto3, "client", _client)

    # Now import and call the real lambda handler
    from lambda_handler import lambda_handler

    result = lambda_handler(event, SimpleNamespace(aws_request_id="test-simple-1"))

    # Basic assertions: status ok and local file was created
    assert result.get("status") == "ok"
    paths = result.get("paths") or result.get("paths", [])
    # normalize to list
    if isinstance(paths, str):
        paths = [paths]
    assert paths, "no output path returned by stubbed to_parquet"

    # Verify each reported path exists under output_dir
    for p in paths:
        ppath = Path(p)
        # If the stub returned a local path string directly, use it; otherwise, convert s3://... -> local
        if not ppath.exists() and p.startswith("s3://"):
            # derive local
            bucket = os.environ.get("BUCKET_NAME")
            key = p.split(f"s3://{bucket}/", 1)[1] if p.startswith(f"s3://{bucket}/") else p.split("s3://", 1)[1]
            ppath = output_dir / key
        assert ppath.exists(), f"expected output file at {ppath}"

    # Clean up env if needed
    # ...no cleanup to keep output for inspection...