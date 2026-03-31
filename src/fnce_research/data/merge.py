"""
Dataset merge utilities.

The most common merge in empirical finance: linking CRSP (permno) to
Compustat (gvkey) via the CCM link table, with date-effective matching.
"""
import pandas as pd
from fnce_research.data.crsp import load_ccm_link


def add_gvkey(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """
    Merge gvkey onto a CRSP data frame that already has permno and a date column.

    Uses the CCM link table with date-effective matching:
      linkdt <= date <= linkenddt  (null linkenddt = still active)

    Parameters
    ----------
    df : DataFrame with columns [permno, <date_col>]
    date_col : name of the date column in df

    Returns
    -------
    df with gvkey column added (rows without a match get NaN gvkey).
    """
    link = load_ccm_link()[["gvkey", "permno", "linkdt", "linkenddt"]].copy()
    link["linkenddt"] = link["linkenddt"].fillna(pd.Timestamp("2099-12-31"))

    merged = df.merge(link, on="permno", how="left")
    in_range = (merged[date_col] >= merged["linkdt"]) & (merged[date_col] <= merged["linkenddt"])
    merged = merged[in_range].drop(columns=["linkdt", "linkenddt"])

    # Handle rare duplicate matches: keep the row with the most recent linkdt
    merged = (
        merged.sort_values(date_col)
        .drop_duplicates(subset=["permno", date_col], keep="last")
    )
    return merged


def add_permno(df: pd.DataFrame, date_col: str = "datadate") -> pd.DataFrame:
    """
    Merge permno onto a Compustat data frame that already has gvkey and a date column.
    Inverse of add_gvkey.
    """
    link = load_ccm_link()[["gvkey", "permno", "linkdt", "linkenddt"]].copy()
    link["linkenddt"] = link["linkenddt"].fillna(pd.Timestamp("2099-12-31"))

    merged = df.merge(link, on="gvkey", how="left")
    in_range = (merged[date_col] >= merged["linkdt"]) & (merged[date_col] <= merged["linkenddt"])
    merged = merged[in_range].drop(columns=["linkdt", "linkenddt"])

    merged = (
        merged.sort_values(date_col)
        .drop_duplicates(subset=["gvkey", date_col], keep="last")
    )
    return merged
