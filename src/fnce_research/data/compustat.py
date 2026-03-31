"""
Compustat fundamentals pulls (comp.funda, comp.fundq, comp.company).

Variable names follow Compustat mnemonics (at, ceq, ni, etc.) so they match
the Compustat data guide directly.
"""
import pandas as pd
from fnce_research.wrds_conn import query
from fnce_research.data import cache


_FUNDA_COLS = """
    gvkey, datadate, fyear, fyr,
    at, lt, ceq, seq, txditc,
    csho, prcc_f,
    ni, ib, oancf, dp,
    sale, revt, cogs, xsga, xrd,
    capx, ppent,
    dltt, dlc, mib,
    act, lct, che,
    indfmt, datafmt, popsrc, consol
"""


def load_funda(start_yr: int = 1963, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    Compustat Annual Fundamentals (comp.funda).
    Filtered to industrial format, standard consolidation.
    """
    if not force and cache.exists("comp", "funda", start_yr, end_yr):
        return cache.read("comp", "funda", start_yr, end_yr)

    print(f"Pulling Compustat funda {start_yr}-{end_yr} from WRDS...")
    df = query(f"""
        SELECT {_FUNDA_COLS}
        FROM comp.funda
        WHERE datadate BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
          AND indfmt  = 'INDL'
          AND datafmt = 'STD'
          AND popsrc  = 'D'
          AND consol  = 'C'
    """, date_cols=["datadate"])
    cache.write(df, "comp", "funda", start_yr, end_yr)
    return df


_FUNDQ_COLS = """
    gvkey, datadate, fyearq, fqtr, rdq,
    atq, ltq, ceqq, seqq,
    niq, ibq, oancfy,
    saleq, revtq, cogsq,
    cheq, actq, lctq,
    indfmt, datafmt, popsrc, consol
"""


def load_fundq(start_yr: int = 1963, end_yr: int = 2023, force: bool = False) -> pd.DataFrame:
    """
    Compustat Quarterly Fundamentals (comp.fundq).
    rdq = earnings announcement date (key for event studies).
    """
    if not force and cache.exists("comp", "fundq", start_yr, end_yr):
        return cache.read("comp", "fundq", start_yr, end_yr)

    print(f"Pulling Compustat fundq {start_yr}-{end_yr} from WRDS...")
    df = query(f"""
        SELECT {_FUNDQ_COLS}
        FROM comp.fundq
        WHERE datadate BETWEEN '{start_yr}-01-01' AND '{end_yr}-12-31'
          AND indfmt  = 'INDL'
          AND datafmt = 'STD'
          AND popsrc  = 'D'
          AND consol  = 'C'
    """, date_cols=["datadate", "rdq"])
    cache.write(df, "comp", "fundq", start_yr, end_yr)
    return df


def load_company(force: bool = False) -> pd.DataFrame:
    """
    Compustat company header file (comp.company).
    Contains SIC, NAICS, state, country, exchange — static identifiers.
    """
    if not force and cache.exists("comp", "company", 0, 0):
        return cache.read("comp", "company", 0, 0)

    print("Pulling Compustat company from WRDS...")
    df = query("""
        SELECT gvkey, conm, tic, cusip, cik,
               sic, naics, state, loc, exchg,
               fyr, costat
        FROM comp.company
    """)
    cache.write(df, "comp", "company", 0, 0)
    return df
