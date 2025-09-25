# schema_cache.py
import os
import logging
import pandas as pd
from typing import Dict

from parquet_helpers import LOG_TAG

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_SCHEMA_BY_TABLE: Dict[str, pd.DataFrame] = {}
_LOADED = False


def _lambda_task_root() -> str:
    """Return Lambda task root (or local fallback when testing)."""
    return os.environ.get("LAMBDA_TASK_ROOT", os.getcwd())


def _schema_csv_path() -> str:
    """Resolve schema.csv path alongside code files."""
    return os.path.join(_lambda_task_root(), "schema.csv")


def _load_schema_csv_from_disk() -> pd.DataFrame:
    """Load schema.csv (db_table_name, db_field_name, df_type)."""
    path = _schema_csv_path()
    if not os.path.exists(path):
        raise RuntimeError(f"❌ schema.csv not found at {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = df.columns.str.lower()
    required = {"db_table_name", "db_field_name", "df_type"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"❌ schema.csv missing columns: {missing}")
    for c in required:
        df[c] = df[c].str.strip()
    logger.info("📦 %s | schema_loaded rows=%s path=%s", LOG_TAG, f"{len(df):,}", path)
    return df


def ensure_schema_cache_loaded() -> None:
    """Load and cache schema rows grouped by table (preserves CSV order)."""
    global _LOADED, _SCHEMA_BY_TABLE
    if _LOADED:
        return
    full = _load_schema_csv_from_disk()
    for tbl, part in full.groupby("db_table_name", sort=False):
        _SCHEMA_BY_TABLE[tbl] = part.reset_index(drop=True)
    _LOADED = True


def get_table_schema_or_raise(table_name: str) -> pd.DataFrame:
    """Return table-specific schema or raise if missing."""
    if not _LOADED:
        ensure_schema_cache_loaded()
    if table_name not in _SCHEMA_BY_TABLE:
        raise RuntimeError(f"❌ No schema found for table '{table_name}' in schema.csv")
    return _SCHEMA_BY_TABLE[table_name]
