#!/usr/bin/env python3
"""
"""
import numpy as np
import pandas as pd

def calculate_order_stats(order_stats: pd.DataFrame,
                            trading_actions: pd.DataFrame,
                            metainfo: pd.Series,
                            tick_sizes: pd.DataFrame,
                            start_microsecond: int,
                            end_microsecond: int) -> pd.DataFrame:
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
            order_stats = order_stats.loc[(order_stats.index < event.timestamp) |
                                      (order_stats.index > event.until)]
    if order_stats.empty:
        return empty_result()

    # time to fill in milliseconds
    order_stats["time_to_fill"] = (order_stats["first_fill_time"] - order_stats.index) / 1000
    time_to_fill = order_stats["time_to_fill"].describe()

    # distance to best price, also in number of ticks
    order_stats["distance_to_best"] = abs(order_stats["price"] - order_stats["best_price"])
    conditions = [(order_stats.price.values >= step.price_start) &
                  (order_stats.price.values < step.price_end)
                  for step in tick_sizes[["price_start", "price_end"]].itertuples()]
    order_stats["tick_size"] = np.piecewise(np.zeros(order_stats.shape[0]),
                                             conditions, tick_sizes.tick_size.values)
    order_stats["distance_in_ticks"] = round(order_stats["distance_to_best"] / order_stats["tick_size"], 2)
    # only look at orders that have been entered at most 1 tick away from best
    columns = ["price", "quantity_entered", "quantity_filled", "distance_in_ticks"]
    close_to_best = order_stats.loc[order_stats.distance_in_ticks <= 1, columns]
    order_stats[["price", "best_price", "distance_to_best", "tick_size"]] /= 10**metainfo.price_decimals
    value_entered = np.sum(close_to_best["price"] * close_to_best["quantity_entered"])
    value_filled = np.sum(close_to_best["price"] * close_to_best["quantity_filled"])
    fill_ratio = value_filled / value_entered

    stats = dict()
    stats["average_time_to_fill"] = time_to_fill["mean"]
    stats["median_time_to_fill"] = time_to_fill["50%"]
    stats["value_entered"] = value_entered
    stats["value_filled"] = value_filled
    stats["fill_ratio"] = fill_ratio

    return stats


def empty_result():
    return dict()
