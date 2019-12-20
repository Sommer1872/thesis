#!/usr/bin/env python3
"""
"""
import gzip
import pickle
from typing import Tuple

from lifelines import KaplanMeierFitter
import numpy as np
import pandas as pd


def save_pickle(group: Tuple[str, pd.DataFrame]) -> None:
    """
    """
    isin: str
    orders: pd.DataFrame
    isin, orders = group

    filename = f"{isin}_times.pickle.gz"
    orders.to_pickle(f"statistics/times/{filename}")

    return None


def compute_survival(group: Tuple[int, pd.DataFrame]) -> None:
    """
    """
    month: int
    orders: pd.DataFrame
    month, orders = group

    for measure in ("time_to_fill", "time_to_removal"):

        model_name = f"{measure}_{month}"

        series = orders[measure].dropna()
        estimate_until = 20e3
        timeline = np.linspace(0, estimate_until, num=100000)

        if series.empty:
            continue

        model = KaplanMeierFitter()
        model.fit(
            durations=series,
            timeline=timeline,
            label=model_name,
        )

        timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
        filename = f"{model_name}_{timestamp}.pickle.gz"
        with gzip.GzipFile(f"statistics/models/{filename}", "wb") as handle:
            pickle.dump(model, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return None
