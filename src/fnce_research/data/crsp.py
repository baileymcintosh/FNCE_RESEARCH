"""
CRSP equity data pulls.

All functions check the local Parquet cache first; pull from WRDS only if missing.
Raw pulls are intentionally minimal — no joins, no business logic — so the cache
faithfully mirrors the WRDS source.
"""
import pandas as pd
from fnce_research.wrds_conn import get_db
from fnce_research.data import cache


def load_msf(start_yr: int = 1963, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    CRSP Monthly Stock File (crsp.msf).
    Columns: permno, date, ret, retx, prc, shrout, vol, cfacpr, cfacshr
    """
    if not force and cache.exists("crsp", "msf", start_yr, end_yr):
        return cache.read("crsp", "msf", start_yr, end_yr)

    print(f"Pulling CRSP MSF {start_yr}-{end_yr} from WRDS...")
    df = get_db().raw_sql(f"""
        SELECT permno, date, ret, retx, prc, shrout, vol, cfacpr, cfacshr
        FROM crsp.msf
        WHERE date BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
    """, date_cols=["date"])
    df["permno"] = df["permno"].astype(int)
    cache.write(df, "crsp", "msf", start_yr, end_yr)
    return df


def load_dsf(start_yr: int = 1993, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    CRSP Daily Stock File (crsp.dsf).
    Columns: permno, date, ret, retx, prc, shrout, vol, cfacpr, cfacshr

    Note: daily file is large (~10M+ rows). Filter to needed permnos downstream
    or restrict the date range to keep cache files manageable.
    """
    if not force and cache.exists("crsp", "dsf", start_yr, end_yr):
        return cache.read("crsp", "dsf", start_yr, end_yr)

    print(f"Pulling CRSP DSF {start_yr}-{end_yr} from WRDS (this may take a while)...")
    df = get_db().raw_sql(f"""
        SELECT permno, date, ret, retx, prc, shrout, vol, cfacpr, cfacshr
        FROM crsp.dsf
        WHERE date BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
    """, date_cols=["date"])
    df["permno"] = df["permno"].astype(int)
    cache.write(df, "crsp", "dsf", start_yr, end_yr)
    return df


def load_msenames(force: bool = False) -> pd.DataFrame:
    """
    CRSP stock names / identifiers (crsp.msenames).
    Columns: permno, namedt, nameendt, ticker, comnam, exchcd, shrcd, siccd
    Used for: exchange/share-type filters, ticker/name lookups, date-effective joins.
    """
    # msenames doesn't depend on a date range, use 0/0 as a sentinel
    if not force and cache.exists("crsp", "msenames", 0, 0):
        return cache.read("crsp", "msenames", 0, 0)

    print("Pulling CRSP msenames from WRDS...")
    df = get_db().raw_sql("""
        SELECT permno, namedt, nameendt, ticker, comnam,
               exchcd, shrcd, siccd, ncusip, cusip
        FROM crsp.msenames
    """, date_cols=["namedt", "nameendt"])
    df["permno"] = df["permno"].astype(int)
    cache.write(df, "crsp", "msenames", 0, 0)
    return df


def load_msedelist(force: bool = False) -> pd.DataFrame:
    """
    CRSP delisting returns (crsp.msedelist).
    Critical for avoiding survivorship bias in monthly return studies.
    """
    if not force and cache.exists("crsp", "msedelist", 0, 0):
        return cache.read("crsp", "msedelist", 0, 0)

    print("Pulling CRSP msedelist from WRDS...")
    df = get_db().raw_sql("""
        SELECT permno, dlstdt, dlret, dlstcd
        FROM crsp.msedelist
    """, date_cols=["dlstdt"])
    df["permno"] = df["permno"].astype(int)
    cache.write(df, "crsp", "msedelist", 0, 0)
    return df


def load_ccm_link(force: bool = False) -> pd.DataFrame:
    """
    CRSP-Compustat Merged link table (crsp.ccmxpf_lnkhist).
    Columns: gvkey, lpermno, linktype, linkprim, linkdt, linkenddt
    Use to map gvkey <-> permno with date-effective joins.
    """
    if not force and cache.exists("crsp", "ccmxpf_lnkhist", 0, 0):
        return cache.read("crsp", "ccmxpf_lnkhist", 0, 0)

    print("Pulling CCM link table from WRDS...")
    df = get_db().raw_sql("""
        SELECT gvkey, lpermno AS permno, lpermco AS permco,
               linktype, linkprim, linkdt, linkenddt
        FROM crsp.ccmxpf_lnkhist
        WHERE linktype IN ('LU', 'LC')
          AND linkprim IN ('P', 'C')
    """, date_cols=["linkdt", "linkenddt"])
    df["permno"] = df["permno"].astype("Int64")  # nullable int (linkenddt may be null)
    cache.write(df, "crsp", "ccmxpf_lnkhist", 0, 0)
    return df
