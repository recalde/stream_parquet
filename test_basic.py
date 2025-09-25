# test_basic.py
"""
Basic tests for DynamoDB AttributeValue serialization/deserialization.
"""

import os
import csv
from decimal import Decimal
from typing import Any, Dict

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