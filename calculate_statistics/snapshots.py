#!/usr/bin/env python3
"""
"""
import pandas as pd

def calculate_snapshot_statistics(snapshots: pd.DataFrame, trading_actions: pd.DataFrame, metainfo: pd.Series) -> pd.DataFrame:
    price_decimals = 10 ** metainfo.price_decimals
    # don't include market orders in spread calculations
    snapshots = snapshots[(snapshots["best_bid"] != 2147483647.0) & (snapshots["best_ask"] != 2147483647.0)].copy()
    snapshots["mid"] = (snapshots["best_ask"] + snapshots["best_bid"]) * 0.5
    snapshots["quoted_spread"] = snapshots["best_ask"] - snapshots["best_bid"]
    snapshots["relative_quoted_spread_bps"] = (snapshots["quoted_spread"] / snapshots["mid"]) * 100
    snapshots[["quoted_spread", "mid"]] /= price_decimals
    snapshots[["best_ask", "best_bid"]] = (snapshots[["best_ask", "best_bid"]] / price_decimals)
    snapshots["depth_at_best"] = snapshots["best_bid_quantity"] + snapshots["best_ask_quantity"]

    # filter based on trading_actions
    if not trading_actions.empty:
        for _, event in trading_actions.iterrows():
            snapshots = snapshots.loc[(snapshots.index < event.timestamp / 1e6) | (snapshots.index > event.until / 1e6)]
    # if there are still strange values, we remove them
    snapshots = snapshots[snapshots["quoted_spread"] >= 0]

    statistics = snapshots.describe()
    return statistics
