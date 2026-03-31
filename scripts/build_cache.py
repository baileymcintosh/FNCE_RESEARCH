"""
Pull raw data from WRDS and write to local Parquet cache.
Run this once to populate data/raw/ before any analysis.

Usage:
    python scripts/build_cache.py
    python scripts/build_cache.py --start 1990 --end 2023
    python scripts/build_cache.py --datasets crsp_msf crsp_msenames  # subset
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fnce_research.data import crsp, compustat, ibes


DATASETS = {
    "crsp_msf":       lambda s, e: crsp.load_msf(s, e),
    "crsp_msenames":  lambda s, e: crsp.load_msenames(),
    "crsp_msedelist": lambda s, e: crsp.load_msedelist(),
    "crsp_ccmlink":   lambda s, e: crsp.load_ccm_link(),
    "comp_funda":     lambda s, e: compustat.load_funda(s, e),
    "comp_fundq":     lambda s, e: compustat.load_fundq(s, e),
    "comp_company":   lambda s, e: compustat.load_company(),
    "ibes_statsumu":  lambda s, e: ibes.load_statsumu(s, e),
}


def main():
    parser = argparse.ArgumentParser(description="Pull WRDS data to local cache.")
    parser.add_argument("--start", type=int, default=1963, help="Start year")
    parser.add_argument("--end",   type=int, default=2023, help="End year")
    parser.add_argument("--datasets", nargs="+", choices=list(DATASETS.keys()),
                        default=list(DATASETS.keys()), help="Which datasets to pull")
    args = parser.parse_args()

    print(f"Building cache: {args.start}-{args.end}")
    print(f"Datasets: {args.datasets}\n")

    for name in args.datasets:
        print(f"[{name}]")
        try:
            DATASETS[name](args.start, args.end)
        except Exception as exc:
            print(f"  ERROR: {exc}")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
