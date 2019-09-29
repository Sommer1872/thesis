#!/usr/bin/env python3
"""
"""
import numpy as np
import pandas as pd

def calculate_effective_statistics(transactions: pd.DataFrame, metainfo:pd.Series, tick_sizes: pd.DataFrame) -> pd.DataFrame:

    transactions["effective_spread"] = 2 * np.abs(transactions["price"] - transactions["mid"])

    # spread leeway using tick sizes and an unequal join
    tick_sizes = tick_sizes.reset_index() / metainfo.price_decimals
    tick_sizes.columns = ["tick_size", "price_start"]
    tick_sizes["price_end"] = tick_sizes["price_start"].shift(fill_value=np.inf)
    # unequal join
    conditions = [(transactions.price.values >= step.price_start) &
                  (transactions.price.values < step.price_end) for step in tick_sizes[["price_start", "price_end"]].itertuples()]
    transactions["tick_size"] = np.piecewise(np.zeros(transactions.shape[0]), conditions, tick_sizes.tick_size.values)
    transactions["spread_leeway"] = round(transactions["effective_spread"] / transactions["tick_size"] - 1, 2)
    statistics = transactions.describe()
    return statistics
