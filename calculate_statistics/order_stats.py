#!/usr/bin/env python3
"""
"""
import warnings

import numpy as np
import pandas as pd


def calculate_order_stats(
    order_stats: pd.DataFrame,
    trading_actions: pd.DataFrame,
    metainfo: pd.Series,
    tick_sizes: pd.DataFrame,
    start_microsecond: int,
    end_microsecond: int,
) -> pd.DataFrame:
    if order_stats.empty:
        return empty_result()
    order_stats.set_index("entry_time", inplace=True)
    order_stats.sort_index(inplace=True)
    # filter for start/end
    order_stats = order_stats.loc[start_microsecond:end_microsecond]
    # remove market orders
    order_stats = order_stats.loc[order_stats["price"] != 2147483647]

    # filter based on trading_actions
    if not trading_actions.empty:
        for _, event in trading_actions.iterrows():
            order_stats = order_stats.loc[
                (order_stats.index < event.timestamp)
                | (order_stats.index > event.until)
            ]
    if order_stats.empty:
        return empty_result()

    # time to fill in milliseconds
    order_stats["time_to_fill"] = (
        order_stats["first_fill_time"] - order_stats.index
    ) / 1000
    time_to_fill = order_stats["time_to_fill"].describe()

    # distance to best price, also in number of ticks
    order_stats["distance_to_best"] = abs(
        order_stats["price"] - order_stats["best_price"]
    )
    conditions = [
        (order_stats.price.values >= step.price_start)
        & (order_stats.price.values < step.price_end)
        for step in tick_sizes[["price_start", "price_end"]].itertuples()
    ]
    order_stats["tick_size"] = np.piecewise(
        np.zeros(order_stats.shape[0]), conditions, tick_sizes.tick_size.values
    )
    order_stats["distance_in_ticks"] = round(
        order_stats["distance_to_best"] / order_stats["tick_size"], 2
    )
    order_stats[["price", "best_price", "distance_to_best", "tick_size"]] /= (
        10 ** metainfo.price_decimals
    )

    # only look at orders that have been entered at most 1 tick away from best
    columns = ["price", "quantity_entered", "quantity_filled", "distance_in_ticks"]
    close_to_best = order_stats.loc[order_stats.distance_in_ticks <= 1, columns]

    close_to_best["value_entered"] = close_to_best["price"] * close_to_best["quantity_entered"]
    close_to_best["value_filled"] = close_to_best["price"] * close_to_best["quantity_filled"]

    stats = dict()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)

        stats["average_time_to_fill"] = time_to_fill.get("mean", np.nan)
        stats["median_time_to_fill"] = time_to_fill.get("50%", np.nan)

        stats["mean_value_entered"] = np.mean(close_to_best["value_entered"])
        stats["median_value_entered"] = np.median(close_to_best["value_entered"])
        stats["total_value_entered"] = np.sum(close_to_best["value_entered"])

        stats["mean_value_filled"] = np.mean(close_to_best["value_filled"])
        stats["median_value_filled"] = np.median(close_to_best["value_filled"])
        stats["total_value_filled"] = np.sum(close_to_best["value_filled"])

        stats["mean_fill_ratio"] = np.mean(close_to_best["value_filled"] / close_to_best["value_entered"])
        stats["median_fill_ratio"] = np.median(close_to_best["value_filled"] / close_to_best["value_entered"])
        if stats["total_value_entered"] > 0:
            stats["total_fill_ratio"] = stats["total_value_filled"] / stats["total_value_entered"]
        else:
            stats["total_fill_ratio"] = 0

    return stats


def empty_result():
    return dict()
