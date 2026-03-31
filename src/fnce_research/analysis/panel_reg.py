"""
Panel regression helpers: Fama-MacBeth and pooled OLS with standard errors.
Wraps linearmodels for cleaner research use.
"""
import pandas as pd
import numpy as np


def fama_macbeth(
    data: pd.DataFrame,
    y: str,
    X: list[str],
    date_col: str = "date",
    lags: int | None = None,
) -> pd.DataFrame:
    """
    Fama-MacBeth (1973) two-pass regression.

    Step 1: Cross-sectional OLS for each date -> coefficient estimates
    Step 2: Time-series mean of coefficients + Newey-West t-stats

    Parameters
    ----------
    data     : panel DataFrame with (entity, date, y, X...)
    y        : dependent variable column name
    X        : list of regressor column names (intercept added automatically)
    date_col : name of date column
    lags     : Newey-West lags for step 2 (None = auto: floor(T^(1/4)))

    Returns
    -------
    DataFrame: index=variable, columns=[mean, std, t_stat, n_months]
    """
    from statsmodels.regression.linear_model import OLS
    from statsmodels.tools import add_constant
    from fnce_research.utils.stats import newey_west_tstat

    monthly_coefs = []
    for date, grp in data.groupby(date_col):
        sub = grp[[y] + X].dropna()
        if len(sub) < len(X) + 5:
            continue
        res = OLS(sub[y], add_constant(sub[X])).fit()
        monthly_coefs.append(res.params)

    coef_df = pd.DataFrame(monthly_coefs)

    if lags is None:
        T = len(coef_df)
        lags = int(np.floor(T ** (1 / 4)))

    rows = []
    for col in coef_df.columns:
        mean, t = newey_west_tstat(coef_df[col], lags=lags)
        rows.append({
            "variable": col,
            "mean": mean,
            "std": coef_df[col].std(),
            "t_stat": t,
            "n_months": len(coef_df),
        })
    return pd.DataFrame(rows).set_index("variable")


def pooled_ols(
    data: pd.DataFrame,
    y: str,
    X: list[str],
    entity_col: str = "permno",
    date_col: str = "date",
    cluster: str = "date",   # "date", "entity", "both", or None
) -> object:
    """
    Pooled OLS with clustered standard errors via linearmodels.

    Returns the fitted model object (call .summary for full output).

    Parameters
    ----------
    cluster : "date" (time clustering), "entity" (firm clustering),
              "both" (double-clustering), or None (OLS SEs)
    """
    from linearmodels.panel import PooledOLS

    panel = data.set_index([entity_col, date_col])
    sub = panel[[y] + X].dropna()

    import statsmodels.api as sm
    exog = sm.add_constant(sub[X])
    model = PooledOLS(sub[y], exog)

    if cluster == "date":
        return model.fit(cov_type="clustered", cluster_time=True)
    elif cluster == "entity":
        return model.fit(cov_type="clustered", cluster_entity=True)
    elif cluster == "both":
        return model.fit(cov_type="clustered", cluster_entity=True, cluster_time=True)
    else:
        return model.fit()
