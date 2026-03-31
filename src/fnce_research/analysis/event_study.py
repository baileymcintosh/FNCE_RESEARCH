"""
Event study: Cumulative Abnormal Returns (CAR) and Buy-and-Hold ARs (BHAR).

Standard approach:
  1. Define event dates per firm
  2. Estimate normal returns via market model over estimation window
  3. Compute ARs in event window, then aggregate to CARs / BHARs
"""
import numpy as np
import pandas as pd


def compute_car(
    returns: pd.DataFrame,
    events: pd.DataFrame,
    estimation_window: tuple[int, int] = (-250, -11),
    event_window: tuple[int, int] = (-1, 1),
    market_col: str = "mktrf",
    ret_col: str = "ret",
    permno_col: str = "permno",
    date_col: str = "date",
) -> pd.DataFrame:
    """
    Compute CARs using the market model.

    Parameters
    ----------
    returns  : daily (or monthly) stock + market returns panel
               columns: [permno, date, ret, mktrf, ...]
    events   : DataFrame with [permno, event_date]
    estimation_window : (start, end) in trading days relative to event
    event_window      : (start, end) in trading days relative to event

    Returns
    -------
    events DataFrame with car, bhar, n_est columns appended.
    """
    from scipy.stats import linregress

    returns = returns.sort_values([permno_col, date_col])
    results = []

    for _, ev in events.iterrows():
        permno = ev[permno_col]
        evdate = ev["event_date"]

        stock = returns[returns[permno_col] == permno].copy()
        if stock.empty:
            continue

        # Index trading days relative to event
        idx = stock[date_col].searchsorted(evdate)
        if idx >= len(stock):
            continue

        est_start = idx + estimation_window[0]
        est_end   = idx + estimation_window[1]
        ev_start  = idx + event_window[0]
        ev_end    = idx + event_window[1]

        if est_start < 0 or ev_end >= len(stock):
            continue

        est = stock.iloc[max(0, est_start): est_end + 1]
        evt = stock.iloc[ev_start: ev_end + 1]

        if len(est) < 50:  # require minimum estimation observations
            continue

        # OLS market model: ret = alpha + beta * mkt
        slope, intercept, *_ = linregress(est[market_col], est[ret_col])
        expected = intercept + slope * evt[market_col]
        ar = evt[ret_col].values - expected.values

        car  = ar.sum()
        bhar = (1 + evt[ret_col]).prod() - (1 + expected).prod()

        row = ev.to_dict()
        row.update({"car": car, "bhar": bhar, "n_est": len(est)})
        results.append(row)

    return pd.DataFrame(results)
