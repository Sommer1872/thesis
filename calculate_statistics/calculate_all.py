#!/usr/bin/env python3
"""
"""

from collections import defaultdict
from typing import Any, Dict, Union

import pandas as pd

from calculate_statistics.best_bid_ask import calculate_best_bid_ask_statistics
from calculate_statistics.best_depths import calculate_best_depth_statistics
from calculate_statistics.snapshots import calculate_snapshot_statistics
from calculate_statistics.effective_stats import calculate_effective_statistics
from calculate_statistics.realized_vola import calculate_realized_vola_stats


def calculate_orderbook_stats(this_day_imi_data) -> Dict[str, Union[str, Dict]]:

    start_microsecond = int(9.25 * 3600e6)
    end_microsecond = int(17.25 * 3600e6)

    # first, nicely format metadata:
    metadata = pd.DataFrame.from_dict(this_day_imi_data.metadata, orient="index")
    string_columns = ["price_type", "isin", "currency", "group"]
    metadata[string_columns] = metadata[string_columns].apply(
        lambda column: column.str.decode("utf-8"))
    metadata[string_columns] = metadata[string_columns].apply(
        lambda column: column.str.strip())
    # keep only BlueChips / Small-/Mid-Caps
    metadata = metadata[metadata["group"].isin(["ACoK", "ABck"])]
    # keep only CHF denoted
    metadata = metadata[metadata["currency"] == "CHF"]

    all_statistics: Dict[int, Dict[str, Any]] = dict()

    # next, we calculate various statistics for each stock:
    for orderbook_no in metadata.index:

        all_statistics[orderbook_no] = dict()
        this_orderbook_stats = all_statistics[orderbook_no]

        metainfo = metadata.loc[orderbook_no]
        price_decimals = 10**metainfo.price_decimals

        # tick sizes
        tick_table_id = int(metainfo.price_tick_table_id)
        tick_sizes = pd.DataFrame.from_dict(
            this_day_imi_data.price_tick_sizes[tick_table_id], orient="index")

        # trading actions (such as stop trading events)
        trading_actions = pd.DataFrame(
            this_day_imi_data.trading_actions[orderbook_no],
            columns=["timestamp", "trading_state", "book_condition"])
        trading_actions = trading_actions[trading_actions["trading_state"] == b"T"]
        if not trading_actions.empty:
            trading_actions["until"] = trading_actions["timestamp"].shift(-1)
            trading_actions = trading_actions[trading_actions["book_condition"] != b"N"]
            trading_actions.dropna(subset=["until"], inplace=True)
            trading_actions["until"] = trading_actions["until"].astype(int)

        # best bid and ask
        best_bid_ask = pd.DataFrame(this_day_imi_data.best_bid_ask[orderbook_no])
        best_bid_ask_stats = calculate_best_bid_ask_statistics(
            best_bid_ask, trading_actions, metainfo, start_microsecond, end_microsecond)
        this_orderbook_stats["best_bid_ask_stats"] = best_bid_ask_stats

        # depth at best
        best_depths = pd.DataFrame(this_day_imi_data.best_depths[orderbook_no])
        best_depth_stats = calculate_best_depth_statistics(best_depths, trading_actions,
                                                           start_microsecond,
                                                           end_microsecond)
        this_orderbook_stats["best_depth_stats"] = best_depth_stats

        # snapshots
        snapshots = pd.DataFrame.from_dict(this_day_imi_data.snapshots[orderbook_no],
                                           orient="index")
        snapshots = snapshots.loc[int(start_microsecond * 1e-6):int(end_microsecond *
                                                                    1e-6)]
        snapshot_stats = calculate_snapshot_statistics(snapshots, trading_actions,
                                                       metainfo)
        this_orderbook_stats["snapshot_stats"] = snapshot_stats

        # transactions
        transactions = pd.DataFrame(this_day_imi_data.transactions[orderbook_no])
        try:
            transactions.set_index("timestamp", inplace=True)
        except KeyError:
            # in case there are no transactions
            continue
        transactions = transactions.loc[start_microsecond:end_microsecond]
        transactions["mid"] = (transactions["best_ask"] +
                               transactions["best_bid"]) * 0.5
        transactions[["price", "best_bid", "best_ask", "mid"]] /= price_decimals

        # effective_spreads
        this_orderbook_stats["effective_stats"] = calculate_effective_statistics(
            transactions, metainfo, tick_sizes)
        # realized volatility
        this_orderbook_stats["realized_vola_stats"] = calculate_realized_vola_stats(
            transactions)

    results = {
        "date": this_day_imi_data.date,
        "all_statistics": all_statistics,
        "metadata": metadata
    }

    return results
