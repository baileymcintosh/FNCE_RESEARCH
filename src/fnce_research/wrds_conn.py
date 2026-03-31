"""
WRDS connection — wrds package with credentials from .env, no interactive prompts.

Usage:
    from fnce_research.wrds_conn import query, close_db

    df = query("SELECT permno, date, ret FROM crsp.msf LIMIT 10")
    df = query("SELECT ...", date_cols=["date"])
    close_db()
"""
import wrds
import pandas as pd
from fnce_research.config import WRDS_USERNAME, WRDS_PASSWORD

_db: wrds.Connection | None = None


def get_db() -> wrds.Connection:
    global _db
    if _db is None or _is_closed(_db):
        if not WRDS_USERNAME or not WRDS_PASSWORD:
            raise EnvironmentError(
                "WRDS_USERNAME and WRDS_PASSWORD must be set in your .env file."
            )
        _db = wrds.Connection(
            wrds_username=WRDS_USERNAME,
            wrds_password=WRDS_PASSWORD,
        )
    return _db


def query(sql: str, date_cols: list[str] | None = None) -> pd.DataFrame:
    """Run SQL against WRDS, return a DataFrame."""
    return get_db().raw_sql(sql, date_cols=date_cols or [])


def close_db() -> None:
    global _db
    if _db is not None:
        try:
            _db.close()
        except Exception:
            pass
        _db = None


def _is_closed(conn: wrds.Connection) -> bool:
    try:
        return conn.connection is None or conn.connection.closed != 0
    except Exception:
        return True
