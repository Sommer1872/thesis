#!/usr/bin/env python3
"""
"""
from typing import Dict
import numpy as np
import pandas as pd


def calculate_best_depth_statistics(
    best_depths: pd.DataFrame,
    trading_actions: pd.DataFrame,
    metainfo: pd.Series,
    start_microsecond: int,
    end_microsecond: int,
) -> Dict[str, float]:

    best_depths.drop_duplicates(
        subset=["timestamp", "book_side"], inplace=True, keep="last"
    )
    best_depths = best_depths.pivot(
        index="timestamp", columns="book_side", values="new_depth_at_best"
    )
    best_depths.columns = [col.decode("utf-8") for col in best_depths.columns]
    del best_depths[" "]
    # filter on start/end times
    best_depths = best_depths.loc[start_microsecond:end_microsecond]

    # forward-fill NaNs
    best_depths.fillna(method="ffill", inplace=True)
    best_depths.fillna(value=0, inplace=True)

    # sum depth at bid and ask
    best_depths["depth_at_best"] = best_depths.sum(axis=1)

    best_depths["depth_at_best"] /= 10 ** metainfo.price_decimals

    # time validity
    best_depths.reset_index(inplace=True)
    best_depths["time_validity"] = (
        best_depths["timestamp"].shift(-1) - best_depths["timestamp"]
    )
    best_depths.set_index("timestamp", drop=True, inplace=True)

    # filter based on trading_actions
    if not trading_actions.empty:
        for _, event in trading_actions.iterrows():
            best_depths = best_depths[
                (best_depths.index < event.timestamp)
                | (best_depths.index > event.until)
            ]

    total_time = best_depths["time_validity"].sum()
    if total_time > 0:
        # time weighted average depth
        twad = (
            np.sum(best_depths["depth_at_best"] * best_depths["time_validity"])
            / total_time
        )
        return {"depth_time_weighted_average": twad}
    else:
        return {"depth_time_weighted_average": np.nan}
