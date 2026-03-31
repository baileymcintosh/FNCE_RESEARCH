"""
WRDS connection factory — module-level singleton.

Usage:
    from fnce_research.wrds_conn import get_db, close_db

    db = get_db()                          # connects (or reuses existing connection)
    df = db.raw_sql("SELECT ...")
    close_db()                             # call when done (e.g. at end of script)

The singleton avoids renegotiating the SSH tunnel on every query, which matters
in notebooks where each cell may call a data function independently.
"""
import wrds
from fnce_research.config import WRDS_USERNAME

_db: wrds.Connection | None = None


def get_db() -> wrds.Connection:
    """Return the active WRDS connection, creating one if necessary."""
    global _db
    if _db is None or _is_closed(_db):
        if not WRDS_USERNAME:
            raise EnvironmentError(
                "WRDS_USERNAME is not set. Add it to your .env file or environment."
            )
        _db = wrds.Connection(wrds_username=WRDS_USERNAME)
    return _db


def close_db() -> None:
    """Close the active connection and reset the singleton."""
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
