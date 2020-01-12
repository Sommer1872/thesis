#!/usr/bin/env python3
"""
"""
from typing import Dict

import numpy as np
import pandas as pd


def calculate_snapshot_statistics(
    snapshots: pd.DataFrame,
    trading_actions: pd.DataFrame,
    tick_sizes: pd.DataFrame,
    metainfo: pd.Series,
) -> Dict[str, float]:
    price_decimals = 10 ** metainfo.price_decimals
    # don't include market orders in spread calculations
    snapshots = snapshots[
        (snapshots["best_bid"] != 2147483647.0)
        & (snapshots["best_ask"] != 2147483647.0)
    ].copy()
    snapshots["mid"] = (snapshots["best_ask"] + snapshots["best_bid"]) * 0.5
    snapshots["quoted_spread"] = snapshots["best_ask"] - snapshots["best_bid"]
    snapshots["relative_quoted_spread_bps"] = (
        snapshots["quoted_spread"] / snapshots["mid"]
    ) * 100
    snapshots[["quoted_spread", "mid"]] /= price_decimals
    snapshots[["best_ask", "best_bid"]] = (
        snapshots[["best_ask", "best_bid"]] / price_decimals
    )
    snapshots["depth_at_best"] = (
        snapshots["best_bid_quantity"] * snapshots["best_bid"]
        + snapshots["best_ask_quantity"] * snapshots["best_ask"]
    )

    # filter based on trading_actions
    if not trading_actions.empty:
        for _, event in trading_actions.iterrows():
            snapshots = snapshots.loc[
                (snapshots.index < event.timestamp / 1e6)
                | (snapshots.index > event.until / 1e6)
            ]
    # if there are still strange values, we remove them
    snapshots = snapshots[snapshots["quoted_spread"] >= 0]

    # convert tick_sizes to CHF
    tick_sizes = tick_sizes.copy() / price_decimals

    # spread leeway
    for side in ["best_bid", "best_ask"]:
        prices = snapshots[side]
        # unequal join
        conditions = [
            (prices.values >= step.price_start) & (prices.values < step.price_end)
            for step in tick_sizes[["price_start", "price_end"]].itertuples()
        ]
        snapshots[f"tick_size_{side}"] = np.piecewise(
            np.zeros(prices.shape[0]), conditions, tick_sizes.tick_size.values
        )
    snapshots["distance_to_B"] = (snapshots["mid"] - snapshots["best_bid"]) / snapshots[
        "tick_size_best_bid"
    ]
    snapshots["distance_to_S"] = (snapshots["best_ask"] - snapshots["mid"]) / snapshots[
        "tick_size_best_ask"
    ]
    snapshots["spread_leeway"] = round(
        snapshots["distance_to_B"] + snapshots["distance_to_S"] - 1
    )

    snapshot_stats = snapshots.describe()

    stats = {
        "quoted_rel_spread_bps_mean": snapshot_stats.loc[
            "mean", "relative_quoted_spread_bps"
        ],
        "quoted_rel_spread_bps_median": snapshot_stats.loc[
            "50%", "relative_quoted_spread_bps"
        ],
        "quoted_spread_leeway_mean": snapshot_stats.loc["mean", "spread_leeway"],
        "tick_size_mean": snapshot_stats.loc["mean", "tick_size_best_bid"],
        "depth_at_best_mean": snapshot_stats.loc["mean", "depth_at_best"],
        "depth_at_best_median": snapshot_stats.loc["50%", "depth_at_best"],
    }

    return stats
