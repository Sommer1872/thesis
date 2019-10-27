#!/usr/bin/env python3
"""
"""
from typing import Dict
import numpy as np
import pandas as pd


def calculate_effective_statistics(
    transactions: pd.DataFrame, metainfo: pd.Series, tick_sizes: pd.DataFrame
) -> Dict[str, float]:

    transactions["q_t"] = np.where(transactions["aggressor"] == 'B', 1, -1)
    transactions["effective_spread"] = transactions["q_t"] * (transactions["price"] - transactions["mid"])
    transactions["relative_effective_spread_bps"] = (
        transactions["effective_spread"] / transactions["mid"]
    ) * 100

    # spread leeway using tick sizes and an unequal join
    price_decimals = 10 ** metainfo.price_decimals
    tick_sizes /= price_decimals
    # unequal join
    conditions = [
        (transactions.price.values >= step.price_start)
        & (transactions.price.values < step.price_end)
        for step in tick_sizes[["price_start", "price_end"]].itertuples()
    ]
    transactions["tick_size"] = np.piecewise(
        np.zeros(transactions.shape[0]), conditions, tick_sizes.tick_size.values
    )
    transactions["spread_leeway"] = round(
        2 * transactions["effective_spread"] / transactions["tick_size"] - 1, 2
    )

    transactions["trade_value"] = transactions["price"] * transactions["size"]

    # group per microsecond to aggregate single trades
    grouped = transactions.groupby(["timestamp", "price", "aggressor"])
    aggregated = grouped[
        ["price", "effective_spread", "relative_effective_spread_bps", "spread_leeway", "tick_size"]
    ].mean()
    aggregated["trade_value"] = grouped["trade_value"].sum()
    aggregated.reset_index(["price", "aggressor"], drop=True, inplace=True)
    agg_stats = aggregated.describe()

    stats = dict()
    stats["turnover"] = aggregated["trade_value"].sum()

    if not agg_stats.empty:
        stats["num_transactions"] = agg_stats.loc["count", "price"]
        stats["eff_spread_mean"] = agg_stats.loc["mean", "effective_spread"]
        stats["eff_spread_median"] = agg_stats.loc["50%", "effective_spread"]
        stats["eff_rel_spread_bps_mean"] = agg_stats.loc["mean", "relative_effective_spread_bps"]
        stats["eff_rel_spread_bps_median"] = agg_stats.loc["50%", "relative_effective_spread_bps"]
        stats["eff_spread_leeway_mean"] = agg_stats.loc["mean", "spread_leeway"]
        stats["eff_spread_leeway_median"] = agg_stats.loc["50%", "spread_leeway"]
        stats["trade_value_mean"] = agg_stats.loc["mean", "trade_value"]
        stats["trade_value_median"] = agg_stats.loc["50%", "trade_value"]
        stats["tick_size_mean"] = agg_stats.loc["mean", "tick_size"]

    return stats
