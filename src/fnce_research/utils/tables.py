"""
Table output helpers: LaTeX and Excel.
"""
from pathlib import Path
import pandas as pd
from fnce_research.config import RESULTS_DIR


def to_latex(
    df: pd.DataFrame,
    filename: str,
    caption: str = "",
    label: str = "",
    float_format: str = "%.3f",
    **kwargs,
) -> Path:
    """
    Write a DataFrame to a .tex file in results/tables/.

    Parameters
    ----------
    filename    : e.g. "table1_portfolio_sorts.tex"
    caption/label : LaTeX caption and label strings
    float_format : format string for floats
    **kwargs    : passed to DataFrame.to_latex()
    """
    path = RESULTS_DIR / "tables" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    tex = df.to_latex(
        float_format=float_format,
        caption=caption,
        label=label,
        **kwargs,
    )
    path.write_text(tex)
    print(f"  Saved LaTeX table -> {path}")
    return path


def to_excel(df: pd.DataFrame, filename: str, sheet_name: str = "Sheet1") -> Path:
    """Write a DataFrame to .xlsx in results/tables/."""
    path = RESULTS_DIR / "tables" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, sheet_name=sheet_name, index=True)
    print(f"  Saved Excel table -> {path}")
    return path
