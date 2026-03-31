"""
Parquet-based local cache for WRDS pulls.

File convention:
    data/raw/<library>/<table>_<start_yr>_<end_yr>.parquet
    e.g. data/raw/crsp/dsf_1990_2023.parquet

The naming encodes provenance so you can tell what a file covers without opening it.
Compression: zstd (good balance of size vs. read speed for analytics workloads).
"""
from pathlib import Path
import pandas as pd
from fnce_research.config import DATA_RAW


def _path(library: str, table: str, start_yr: int, end_yr: int) -> Path:
    return DATA_RAW / library / f"{table}_{start_yr}_{end_yr}.parquet"


def exists(library: str, table: str, start_yr: int, end_yr: int) -> bool:
    return _path(library, table, start_yr, end_yr).exists()


def read(library: str, table: str, start_yr: int, end_yr: int) -> pd.DataFrame:
    p = _path(library, table, start_yr, end_yr)
    if not p.exists():
        raise FileNotFoundError(f"Cache miss: {p}")
    return pd.read_parquet(p)


def write(df: pd.DataFrame, library: str, table: str, start_yr: int, end_yr: int) -> Path:
    p = _path(library, table, start_yr, end_yr)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False, engine="pyarrow", compression="zstd")
    print(f"  Cached {len(df):,} rows -> {p.relative_to(DATA_RAW.parent.parent)}")
    return p


def invalidate(library: str, table: str, start_yr: int, end_yr: int) -> None:
    """Delete a cached file so it will be re-pulled on next access."""
    p = _path(library, table, start_yr, end_yr)
    if p.exists():
        p.unlink()
        print(f"  Deleted cache: {p}")
    else:
        print(f"  No cache found: {p}")
