# FNCE Research

Empirical finance research toolkit. Connects to WRDS for data, caches locally as Parquet.

## Setup

```bash
# 1. Activate the conda environment
conda activate fnce_research

# 2. (First time only) Install from source
#    Already done if you ran the setup — skip if fnce-research appears in pip list
pip install -e ".[dev]"

# 3. Fill in your WRDS username
#    Edit .env and set: WRDS_USERNAME=your_wrds_username

# 4. Verify everything works
jupyter lab notebooks/00_setup_check.ipynb
```

On first WRDS connection, you'll be prompted for your password. It gets saved to
`~/.pgpass` so subsequent connections are silent.

## Structure

```
src/fnce_research/
  config.py           # paths, constants, env vars
  wrds_conn.py        # WRDS connection singleton
  data/
    cache.py          # Parquet read/write helpers
    crsp.py           # CRSP equity data
    compustat.py      # Compustat fundamentals
    ibes.py           # IBES analyst forecasts
    merge.py          # CRSP-Compustat link table utilities
  analysis/
    portfolio_sorts.py  # univariate/bivariate sorts
    event_study.py      # CAR / BHAR computation
    panel_reg.py        # Fama-MacBeth, pooled OLS
  utils/
    stats.py          # Newey-West t-stats, winsorize, standardize
    tables.py         # LaTeX / Excel table output

notebooks/
  00_setup_check.ipynb    # verify connection + cache
  01_data_pull_crsp.ipynb # CRSP pull walkthrough

scripts/
  build_cache.py      # pull all WRDS data to data/raw/

data/
  raw/       # Parquet files from WRDS (gitignored)
  processed/ # merged/cleaned datasets (gitignored)
  manual/    # hand-curated files (committed)

results/
  tables/    # .tex / .xlsx output
  figures/   # .pdf / .png output
```

## Pulling data

```bash
# Pull everything (takes a while on first run)
python scripts/build_cache.py --start 1963 --end 2023

# Pull only specific datasets
python scripts/build_cache.py --datasets crsp_msf crsp_msenames comp_funda
```

## Key conventions

- **Cache-first**: every data function checks `data/raw/` before hitting WRDS
- **Source-faithful raw cache**: no joins or business logic in `data/`; clean in analysis code
- **NYSE breakpoints**: sort functions default to NYSE-only breakpoints (FF convention)
- **WRDS data is gitignored**: licensing prohibits redistribution; `build_cache.py` is the reproducibility contract

## Environment

- Conda env: `fnce_research` (Python 3.11)
- Jupyter kernel: "FNCE Research"
- Key packages: `wrds`, `pandas`, `linearmodels`, `statsmodels`, `pyarrow`
