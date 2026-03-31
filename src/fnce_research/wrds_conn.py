"""
WRDS connection — direct PostgreSQL, no SSH tunnel, no interactive prompts.

Credentials come from .env (WRDS_USERNAME + WRDS_PASSWORD).
Returns a singleton sqlalchemy engine; all data functions use db.raw_sql()
which accepts the engine directly.

Usage:
    from fnce_research.wrds_conn import get_db, query, close_db

    # Option A — use the wrds-style raw_sql wrapper (same API as before)
    df = query("SELECT permno, date, ret FROM crsp.msf LIMIT 10")

    # Option B — use the engine directly with pandas
    import pandas as pd
    df = pd.read_sql("SELECT ...", get_db())
"""
import pandas as pd
import sqlalchemy
from fnce_research.config import WRDS_USERNAME, WRDS_PASSWORD

_engine: sqlalchemy.engine.Engine | None = None

WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = 9737
WRDS_DB   = "wrds"


def get_db() -> sqlalchemy.engine.Engine:
    """Return the active SQLAlchemy engine, creating one if necessary."""
    global _engine
    if _engine is None:
        if not WRDS_USERNAME or not WRDS_PASSWORD:
            raise EnvironmentError(
                "WRDS_USERNAME and WRDS_PASSWORD must be set in your .env file."
            )
        url = sqlalchemy.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=WRDS_USERNAME,
            password=WRDS_PASSWORD,
            host=WRDS_HOST,
            port=WRDS_PORT,
            database=WRDS_DB,
        )
        _engine = sqlalchemy.create_engine(
            url,
            connect_args={"sslmode": "require"},
            pool_pre_ping=True,   # reconnects automatically if connection drops
        )
    return _engine


def query(sql: str, date_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Run a SQL query against WRDS and return a DataFrame.

    Parameters
    ----------
    sql       : SQL string (use %s-style params or f-strings for dynamic queries)
    date_cols : column names to parse as datetime

    Examples
    --------
    df = query("SELECT permno, date, ret FROM crsp.msf WHERE date = '2023-12-29'")
    """
    df = pd.read_sql(sql, get_db())
    if date_cols:
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
    return df


def close_db() -> None:
    """Dispose the connection pool."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
