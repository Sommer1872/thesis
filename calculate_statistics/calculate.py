#!/usr/bin/env python3
"""
"""

import sys

sys.path.append("..")

import numpy as np
import pandas as pd

def calculate_orderbook_stats(this_day_imi_data):

    start_microsecond = int(9.25 * 3600e6)
    end_microsecond = int(17.25 * 3600e6)

    # first, nicely format metadata:
    metadata = pd.DataFrame.from_dict(this_day_imi_data.metadata, orient="index")
    string_columns = ["price_type", "isin", "currency", "group"]
    metadata[string_columns] = metadata[string_columns].apply(
        lambda column: column.str.decode("utf-8")
    )
    metadata[string_columns] = metadata[string_columns].apply(
        lambda column: column.str.strip()
    )
    # keep only BlueChips
    metadata = metadata[metadata["group"] == "ACoK"]

    all_orderbook_stats = dict()
    all_transaction_stats = dict()

    # next, we process the orderbook for each stock:
    for orderbook_no in metadata.index:
        price_decimals = 10 ** metadata.loc[orderbook_no].price_decimals
        stats = pd.DataFrame.from_dict(this_day_imi_data.orderbook_stats[orderbook_no], orient="index")
        stats = stats.loc[int(start_microsecond*1e-6):int(end_microsecond*1e-6)]
        stats["quoted_spread"] = stats["best_ask"] - stats["best_bid"]
        stats["mid"] = (stats["best_ask"] + stats["best_bid"]) * 0.5
        stats["relative_quoted_spread_bps"] = 100 * stats["quoted_spread"] / stats["mid"]
        stats[["quoted_spread", "mid"]] /= price_decimals
        stats[["best_ask", "best_bid"]] = (stats[["best_ask", "best_bid"]] / price_decimals)

        all_orderbook_stats[orderbook_no] = stats.describe()

        # effective spreads
        transactions = pd.DataFrame(this_day_imi_data.transactions[orderbook_no]).set_index("timestamp")
        transactions = transactions.loc[start_microsecond:end_microsecond]
        transactions["mid"] = (transactions["best_ask"] + transactions["best_bid"]) * 0.5
        transactions["relative_effective_spread_bps"] = 100 * 2 * np.abs(transactions["price"] - transactions["mid"]) / transactions["mid"]
        transactions[["price", "best_bid", "best_ask", "mid"]] /= price_decimals
        transactions["effective_spread"] = 2 * np.abs(transactions["price"] - transactions["mid"])

        # spread leeway using tick sizes and an unequal join
        tick_table_id = int(metadata.loc[orderbook_no].price_tick_table_id)
        tick_sizes = pd.DataFrame.from_dict(this_day_imi_data.price_tick_sizes[tick_table_id], orient="index")
        tick_sizes = tick_sizes.reset_index() / price_decimals
        tick_sizes.columns = ["tick_size", "price_start"]
        tick_sizes["price_end"] = tick_sizes["price_start"].shift(fill_value=np.inf)
        # unequal join
        conditions = [(transactions.price.values >= step.price_start) &
                      (transactions.price.values < step.price_end) for step in tick_sizes[["price_start", "price_end"]].itertuples()]
        transactions["tick_size"] = np.piecewise(np.zeros(transactions.shape[0]), conditions, tick_sizes.tick_size.values)
        transactions["spread_leeway"] = transactions["effective_spread"] / transactions["tick_size"]

        all_transaction_stats[orderbook_no] = transactions.describe()

    return this_day_imi_data.date, all_orderbook_stats, all_transaction_stats, metadata
