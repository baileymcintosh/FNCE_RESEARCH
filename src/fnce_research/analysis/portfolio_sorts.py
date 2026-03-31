"""
Univariate and bivariate portfolio sorts.

Standard empirical finance workflow:
  1. Sort stocks into N portfolios by a signal at time t
  2. Hold for one period, measure returns at t+1
  3. Report mean returns, alphas, and long-short spread
"""
import numpy as np
import pandas as pd
from fnce_research.utils.stats import newey_west_tstat


def univariate_sort(
    data: pd.DataFrame,
    signal_col: str,
    ret_col: str,
    date_col: str = "date",
    n_portfolios: int = 10,
    weight: str = "ew",          # "ew" (equal-weight) or "vw" (value-weight)
    weight_col: str = "me",      # market cap column, used only when weight="vw"
    breakpoint_col: str | None = None,  # if set, use only these rows for breakpoints
) -> pd.DataFrame:
    """
    Monthly univariate portfolio sort.

    Parameters
    ----------
    data          : panel DataFrame with (permno, date, signal, ret, [me])
    signal_col    : column to sort on (measured at t, before the return period)
    ret_col       : return column (measured at t+1, i.e. already shifted)
    n_portfolios  : number of portfolios (10 = deciles, 5 = quintiles)
    weight        : "ew" or "vw"
    breakpoint_col: boolean column; if provided, breakpoints set on rows where True
                    (FF convention: NYSE-only breakpoints)

    Returns
    -------
    DataFrame: index=date, columns=[p1, p2, ..., pN, ls]
      ls = pN - p1 (high signal minus low signal)
    """
    results = []

    for date, grp in data.groupby(date_col):
        grp = grp.dropna(subset=[signal_col, ret_col])
        if len(grp) < n_portfolios * 2:
            continue

        # Compute breakpoints
        bp_data = grp[grp[breakpoint_col]] if breakpoint_col else grp
        breakpoints = bp_data[signal_col].quantile(
            np.linspace(0, 1, n_portfolios + 1)
        ).values

        labels = list(range(1, n_portfolios + 1))
        grp = grp.copy()
        grp["_port"] = pd.cut(
            grp[signal_col],
            bins=breakpoints,
            labels=labels,
            include_lowest=True,
        ).astype("Int64")

        port_rets = {}
        for p in labels:
            sub = grp[grp["_port"] == p]
            if len(sub) == 0:
                port_rets[f"p{p}"] = np.nan
            elif weight == "vw":
                w = sub[weight_col].clip(lower=0)
                port_rets[f"p{p}"] = (
                    np.average(sub[ret_col], weights=w) if w.sum() > 0 else np.nan
                )
            else:
                port_rets[f"p{p}"] = sub[ret_col].mean()

        port_rets["ls"] = port_rets.get(f"p{n_portfolios}", np.nan) - port_rets.get("p1", np.nan)
        port_rets[date_col] = date
        results.append(port_rets)

    out = pd.DataFrame(results).set_index(date_col)
    return out


def sort_summary(sort_returns: pd.DataFrame, lags: int | None = None) -> pd.DataFrame:
    """
    Summarize portfolio sort output: mean return and NW t-stat for each portfolio.

    Parameters
    ----------
    sort_returns : output of univariate_sort()
    lags         : Newey-West lags (None = auto)

    Returns
    -------
    DataFrame with columns [mean, t_stat] and portfolio labels as index.
    """
    rows = []
    for col in sort_returns.columns:
        mean, t = newey_west_tstat(sort_returns[col], lags=lags)
        rows.append({"portfolio": col, "mean": mean, "t_stat": t})
    return pd.DataFrame(rows).set_index("portfolio")
