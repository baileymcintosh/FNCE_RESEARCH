# Quick connection test — run this from the terminal after logging into
# https://wrds-www.wharton.upenn.edu and approving the Duo push.
#
# Usage:
#   conda activate fnce_research
#   python test_conn.py

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from fnce_research.wrds_conn import connect, query, close

connect()  # approve Duo push on your phone when it arrives

df = query(
    "SELECT permno, date, ret FROM crsp.msf WHERE date='2023-12-29' LIMIT 5",
    date_cols=["date"]
)
print(f"SUCCESS — {len(df)} rows:")
print(df.to_string())

close()
