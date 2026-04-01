"""
WRDS connection.

IMPORTANT — WRDS requires Duo MFA on every connection attempt.
Follow this workflow each session to avoid account lockout:

  1. Open https://wrds-www.wharton.upenn.edu in your browser
  2. Log in and approve the Duo push on your phone
  3. WITHOUT switching networks or VPN, run your code from this same machine
  4. Call connect() once per session — do not retry in a loop

Usage:
    from fnce_research.wrds_conn import connect, query, close

    connect()          # call once — triggers Duo push, approve on phone
    df = query("SELECT permno, date, ret FROM crsp.msf LIMIT 10")
    close()
"""
import wrds
import pandas as pd
from fnce_research.config import WRDS_USERNAME, WRDS_PASSWORD

_db: wrds.Connection | None = None


def connect() -> None:
    """
    Open the WRDS connection for this session.

    Call this once after logging into the WRDS website and approving Duo.
    Do NOT call this in a loop or from automated/unattended scripts.
    """
    global _db
    if _db is not None and not _is_closed(_db):
        print("Already connected.")
        return
    if not WRDS_USERNAME or not WRDS_PASSWORD:
        raise EnvironmentError(
            "WRDS_USERNAME and WRDS_PASSWORD must be set in your .env file."
        )
    print("Connecting to WRDS...")
    _db = wrds.Connection(
        wrds_username=WRDS_USERNAME,
        wrds_password=WRDS_PASSWORD,
        autoconnect=False,   # skip the slow load_library_list() call
    )
    _db.connect()
    print("Connected.")


def query(sql: str, date_cols: list[str] | None = None) -> pd.DataFrame:
    """Run SQL against WRDS, return a DataFrame. Call connect() first."""
    if _db is None or _is_closed(_db):
        raise RuntimeError("Not connected. Call connect() first.")
    return _db.raw_sql(sql, date_cols=date_cols or [])


def close() -> None:
    """Close the WRDS connection at the end of your session."""
    global _db
    if _db is not None:
        try:
            _db.close()
        except Exception:
            pass
        _db = None
        print("Connection closed.")


def _is_closed(conn: wrds.Connection) -> bool:
    try:
        return conn.connection is None or conn.connection.closed != 0
    except Exception:
        return True
