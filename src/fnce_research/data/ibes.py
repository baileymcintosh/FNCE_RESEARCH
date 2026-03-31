"""
IBES analyst forecast data (ibes library on WRDS).

Key tables:
  ibes.statsumu  — summary statistics (consensus EPS forecasts)
  ibes.det_epsus — individual analyst forecasts (US, EPS)
  ibes.actu_epsus — actuals

All pulls are cache-first.
"""
import pandas as pd
from fnce_research.wrds_conn import get_db
from fnce_research.data import cache


def load_statsumu(start_yr: int = 1976, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    IBES consensus EPS summary statistics (ibes.statsumu).
    Columns: ticker, statpers, fiscalp, fpi, meanest, medest, stdev, numest, anndats_act, actual
    """
    if not force and cache.exists("ibes", "statsumu", start_yr, end_yr):
        return cache.read("ibes", "statsumu", start_yr, end_yr)

    print(f"Pulling IBES statsumu {start_yr}-{end_yr} from WRDS...")
    df = get_db().raw_sql(f"""
        SELECT ticker, cusip, statpers, fiscalp, fpi,
               meanest, medest, stdev, numest,
               anndats_act, actual
        FROM ibes.statsumu
        WHERE statpers BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
          AND fpi IN ('1', '2', '3', '4')   -- 1-4 quarter-ahead forecasts
          AND fiscalp = 'QTR'
    """, date_cols=["statpers", "anndats_act"])
    cache.write(df, "ibes", "statsumu", start_yr, end_yr)
    return df


def load_actuals(start_yr: int = 1976, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    IBES actual EPS (ibes.actu_epsus).
    Use for computing earnings surprises: (actual - consensus) / |consensus|
    """
    if not force and cache.exists("ibes", "actu_epsus", start_yr, end_yr):
        return cache.read("ibes", "actu_epsus", start_yr, end_yr)

    print(f"Pulling IBES actuals {start_yr}-{end_yr} from WRDS...")
    df = get_db().raw_sql(f"""
        SELECT ticker, cusip, pdicity, pends, anndats, actual, surp
        FROM ibes.actu_epsus
        WHERE anndats BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
          AND pdicity = 'QTR'
    """, date_cols=["pends", "anndats"])
    cache.write(df, "ibes", "actu_epsus", start_yr, end_yr)
    return df
