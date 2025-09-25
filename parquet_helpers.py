# parquet_helpers.py
import os
import math
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
from boto3.dynamodb.types import TypeDeserializer
import json
import base64
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Stable tag for grep/alerts
LOG_TAG = "DDB→Parquet"

# Module-level caches
_DESERIALIZER = TypeDeserializer()
_EST_TZ: ZoneInfo | None = None  # configurable via TIMEZONE env


# ----- Human-readable helpers -----
def human_readable_bytes(n: int | None) -> str:
    """Return bytes in friendly units (e.g., 12.3 MB)."""
    if not n:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = min(int(math.log(n, 1024)), len(units) - 1)
    return f"{round(n / (1024 ** i), 2)} {units[i]}"


def human_readable_rows(n: int) -> str:
    """Return row count with thousands separators."""
    return f"{n:,} rows"


# ----- Timezone & S3 key helpers -----
def est_tz() -> ZoneInfo:
    """Return the local timezone (defaults to America/New_York)."""
    global _EST_TZ
    if _EST_TZ is None:
        _EST_TZ = ZoneInfo(os.environ.get("TIMEZONE", "America/New_York"))
    return _EST_TZ


def build_s3_key_with_local_hour(base_prefix: str, table: str, event_dt_utc: datetime) -> str:
    """Build S3 key {base}/{table}/YYYY/MM/DD/HH/{table}_{ts}_{uid}.parquet using local hour."""
    local = event_dt_utc.astimezone(est_tz())
    yyyy, mm, dd, hh = f"{local.year:04d}", f"{local.month:02d}", f"{local.day:02d}", f"{local.hour:02d}"
    uid = uuid.uuid4().hex[:8]
    ts = local.strftime("%Y%m%dT%H%M%S")
    base = base_prefix.rstrip("/")
    return f"{base}/{table}/{yyyy}/{mm}/{dd}/{hh}/{table}_{ts}_{uid}.parquet"


# ----- DynamoDB stream helpers -----
def get_table_name_from_stream_arn(event_source_arn: str) -> str:
    """Extract table name from DDB Streams ARN."""
    try:
        return event_source_arn.split(":table/")[1].split("/")[0]
    except Exception:
        return "unknown"


def get_batch_event_time_utc(records: List[Dict[str, Any]]) -> datetime:
    """Use the max ApproximateCreationDateTime across records; fallback to now UTC."""
    ts: List[datetime] = []
    for r in records:
        t = r.get("dynamodb", {}).get("ApproximateCreationDateTime")
        if isinstance(t, (int, float)):
            ts.append(datetime.fromtimestamp(t, tz=timezone.utc))
    return max(ts) if ts else datetime.now(tz=timezone.utc)


def deserialize_stream_new_image(record: Dict[str, Any]) -> Dict[str, Any]:
    """Turn NewImage AttributeValue map into plain Python dict."""
    new_img = record.get("dynamodb", {}).get("NewImage")
    if not new_img:
        return {}
    return _DESERIALIZER.deserialize({"M": new_img}) or {}


# ----- Schema & typing helpers -----
def _normalize_for_json(obj):
    """Recursively convert values to JSON-safe types:
    - Decimal -> str (preserve exactness)
    - bytes -> base64 string
    - dict/list -> recurse
    """
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (int, float, str, bool)):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    if isinstance(obj, dict):
        return {str(k): _normalize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize_for_json(v) for v in obj]
    # fallback to string
    return str(obj)


def _arrow_type_for(df_type: str) -> pa.DataType:
    """Map df_type string to a PyArrow type."""
    f = (df_type or "").strip().lower()
    if f in ("string", "str", "varchar", "text", "object"):
        return pa.string()
    if f in ("int64", "bigint"):
        return pa.int64()
    if f in ("int32", "int"):
        return pa.int32()
    if f in ("float64", "double", "float"):
        return pa.float64()
    if f in ("bool", "boolean"):
        return pa.bool_()
    if f in ("timestamp", "datetime", "datetime64[ns]"):
        return pa.timestamp("ns", tz="UTC")  # store UTC; partition path is local time
    if f == "date":
        return pa.date32()
    if f == "decimal":
        prec = int(os.environ.get("DECIMAL_PRECISION", "38"))
        scale = int(os.environ.get("DECIMAL_SCALE", "10"))
        return pa.decimal128(prec, scale)
    if f in ("bytes", "binary", "blob"):
        return pa.binary()
    if f in ("list", "dict", "map", "json"):
        # We'll serialize these to JSON strings for Arrow
        return pa.string()
    if f in ("null",):
        return pa.null()
    return pa.string()


def coerce_dataframe_types_with_schema(df: pd.DataFrame, table_schema: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, pa.DataType]]:
    """Reorder/create columns to match schema; return (df, dtype_map) for awswrangler."""
    want_cols = table_schema["db_field_name"].tolist()

    # Ensure presence and order
    for c in want_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[want_cols]

    dtype_map: Dict[str, pa.DataType] = {}
    table_name = table_schema.iloc[0]["db_table_name"]

    # Light pandas-side coercion to reduce write errors
    for _, row in table_schema.iterrows():
        col = row["db_field_name"]
        df_type = (row["df_type"] or "").lower()
        dtype_map[col] = _arrow_type_for(df_type)
        try:
            if df_type in ("int64", "bigint"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif df_type in ("int32", "int"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
            elif df_type in ("float64", "double", "float"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif df_type in ("bool", "boolean"):
                df[col] = df[col].astype("boolean")
            elif df_type in ("timestamp", "datetime", "datetime64[ns]"):
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            elif df_type == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif df_type in ("list", "dict", "map", "object", "json"):
                # Serialize nested structures to JSON strings after normalizing Decimals/bytes
                def _to_json_safe(v):
                    if pd.isna(v):
                        return None
                    return json.dumps(_normalize_for_json(v), ensure_ascii=False)
                df[col] = df[col].apply(_to_json_safe)
            elif df_type in ("bytes", "binary", "blob"):
                # Ensure bytes-like objects (keep existing bytes, encode strings)
                def _ensure_bytes(v):
                    if pd.isna(v):
                        return None
                    if isinstance(v, (bytes, bytearray)):
                        return bytes(v)
                    if isinstance(v, str):
                        return v.encode("utf-8")
                    return v
                df[col] = df[col].apply(_ensure_bytes)
            # decimal: Arrow enforces on write
        except Exception as ex:
            logger.warning("⚠️ %s | coercion_warn table=%s col=%s err=%s", LOG_TAG, table_name, col, ex)

    return df, dtype_map
