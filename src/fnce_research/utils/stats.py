"""
Statistical utilities: Newey-West t-stats, winsorizing, standardizing.
"""
import numpy as np
import pandas as pd
from statsmodels.stats.sandwich_covariance import cov_hac


def newey_west_tstat(series: pd.Series, lags: int | None = None) -> tuple[float, float]:
    """
    Compute mean and Newey-West t-statistic for a return series.

    Parameters
    ----------
    series : time series of returns (e.g. monthly long-short portfolio)
    lags   : number of lags for NW correction. Default = floor(4*(T/100)^(2/9)).

    Returns
    -------
    (mean, t_stat)
    """
    import statsmodels.api as sm

    s = series.dropna()
    T = len(s)
    if lags is None:
        lags = int(np.floor(4 * (T / 100) ** (2 / 9)))

    X = sm.add_constant(np.ones(T))
    res = sm.OLS(s.values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    return float(res.params[0]), float(res.tvalues[0])


def winsorize(
    df: pd.DataFrame,
    cols: list[str],
    q: float = 0.01,
    by: str | None = None,
) -> pd.DataFrame:
    """
    Winsorize columns at [q, 1-q] quantiles, optionally within groups (e.g. by date).

    Parameters
    ----------
    df   : DataFrame
    cols : list of column names to winsorize
    q    : tail fraction to clip (0.01 = 1st and 99th percentile)
    by   : column name to group by before computing quantiles (e.g. 'date')
    """
    df = df.copy()
    for col in cols:
        if by:
            lo = df.groupby(by)[col].transform(lambda x: x.quantile(q))
            hi = df.groupby(by)[col].transform(lambda x: x.quantile(1 - q))
        else:
            lo = df[col].quantile(q)
            hi = df[col].quantile(1 - q)
        df[col] = df[col].clip(lower=lo, upper=hi)
    return df


def standardize(series: pd.Series) -> pd.Series:
    """Cross-sectionally standardize: (x - mean) / std."""
    return (series - series.mean()) / series.std()
