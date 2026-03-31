"""
Central configuration: paths, constants, environment variables.

All other modules import from here — no path hardcoding elsewhere.
"""
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# --- Root paths ---
REPO_ROOT = Path(__file__).resolve().parents[2]

_data_base = Path(os.getenv("DATA_DIR", str(REPO_ROOT / "data")))
DATA_RAW    = _data_base / "raw"
DATA_PROC   = _data_base / "processed"
DATA_MANUAL = _data_base / "manual"
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", str(REPO_ROOT / "results")))

# Ensure directories exist at import time
for _d in [DATA_RAW, DATA_PROC, DATA_MANUAL, RESULTS_DIR / "tables", RESULTS_DIR / "figures"]:
    _d.mkdir(parents=True, exist_ok=True)

# --- WRDS ---
WRDS_USERNAME: str | None = os.getenv("WRDS_USERNAME")

# --- Sample period defaults ---
SAMPLE_START = "1963-07-01"   # standard start for CRSP anomaly tests (post-COMPUSTAT merge)
SAMPLE_END   = "2023-12-31"   # update as data refreshes

# --- Cross-sectional sort conventions ---
# FF convention: use NYSE stocks only to set size/BM breakpoints
NYSE_BREAKPOINTS = True
