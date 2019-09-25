#!/usr/bin/env python3
"""
"""
from typing import Dict
import sys

sys.path.append("..")

import numpy as np
import pandas as pd

def calculate_orderbook_stats(this_day_imi_data) -> Dict[str, pd.DataFrame]:

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
    all_price_impact_stats = dict()
    all_time_weighted_stats = dict()

    # next, we process the orderbook for each stock:
    for orderbook_no in metadata.index:
        price_decimals = 10 ** metadata.loc[orderbook_no].price_decimals

        # quoted spreads
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
        transactions["spread_leeway"] = round(transactions["effective_spread"] / transactions["tick_size"] - 1, 2)
        all_transaction_stats[orderbook_no] = transactions.describe()

        # best bid and ask
        best_bid_ask = pd.DataFrame(this_day_imi_data.best_bid_ask[orderbook_no])
        best_bid_ask.drop_duplicates(subset=["timestamp", "book_side"], inplace=True, keep="last")
        # show bids/asks side by side
        best_bid_ask = best_bid_ask.pivot(index="timestamp", columns="book_side", values="new_best_price")
        best_bid_ask.columns = [col.decode("utf-8") for col in best_bid_ask.columns]
        del best_bid_ask[" "]
        # get prices in CHF
        best_bid_ask = best_bid_ask / price_decimals
        # remember cases when there are not orders on both sides of the order book
        missing = best_bid_ask.isna().all(axis=1)
        best_bid_ask.fillna(method="ffill", inplace=True)
        # in missing cases from above, we set nan's to bid/ask/mid
        best_bid_ask[missing] = np.nan
        best_bid_ask["quoted_spread"] = best_bid_ask["S"] - best_bid_ask["B"]
        best_bid_ask.reset_index(inplace=True)
        best_bid_ask["time_validity"] = best_bid_ask["timestamp"].shift(-1) - best_bid_ask["timestamp"]
        best_bid_ask.set_index("timestamp", drop=True, inplace=True)
        # filter based on start / end time and get prices in CHF
        best_bid_ask = best_bid_ask.loc[start_microsecond:end_microsecond]

        time_weighted_quoted_spread = np.sum(best_bid_ask["quoted_spread"] * best_bid_ask["time_validity"]) / best_bid_ask["time_validity"].sum()
        # best_bid_ask["mid_price"] = (best_bid_ask["B"] + best_bid_ask["S"]) * 0.5
        all_time_weighted_stats[orderbook_no] = time_weighted_quoted_spread

    results = dict("date": this_day_imi_data.date,
        "all_orderbook_stats": all_orderbook_stats,
        "all_transaction_stats": all_transaction_stats,
        "all_time_weighted_stats": all_time_weighted_stats,
        "metadata": metadata)

    return results
