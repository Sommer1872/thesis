#!/usr/bin/env python3
"""
"""
import pandas as pd
import numpy as np

from .best_bid_ask import calculate_best_bid_ask_statistics
from .best_depths import calculate_best_depth_statistics
from .order_stats import calculate_order_stats
from .snapshots import calculate_snapshot_statistics
from .trade_stats import calculate_effective_statistics
from .realized_vola import calculate_realized_vola_stats


def calculate_orderbook_stats(this_day_imi_data) -> pd.DataFrame:

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
    # keep only BlueChips / Small-/Mid-Caps
    metadata = metadata[metadata["group"].isin(["ACoK", "ABck"])]
    # keep only CHF denoted
    metadata = metadata[metadata["currency"] == "CHF"]

    # next, we calculate various statistics for each stock:
    for orderbook_no in metadata.index:

        this_orderbook_stats = dict()

        metainfo = metadata.loc[orderbook_no]

        # tick sizes
        tick_table_id = int(metainfo.price_tick_table_id)
        tick_sizes = pd.DataFrame.from_dict(
            this_day_imi_data.price_tick_sizes[tick_table_id], orient="index"
        )
        tick_sizes = tick_sizes.reset_index()
        tick_sizes.columns = ["tick_size", "price_start"]
        tick_sizes["price_end"] = tick_sizes["price_start"].shift(fill_value=np.inf)

        # trading actions (such as stop trading events)
        trading_actions = pd.DataFrame(
            this_day_imi_data.trading_actions[orderbook_no],
            columns=["timestamp", "trading_state", "book_condition"],
        )
        trading_actions = trading_actions[trading_actions["trading_state"] == b"T"]
        if not trading_actions.empty:
            trading_actions["until"] = trading_actions["timestamp"].shift(-1)
            trading_actions = trading_actions[trading_actions["book_condition"] != b"N"]
            trading_actions.dropna(subset=["until"], inplace=True)
            trading_actions["until"] = trading_actions["until"].astype(int)

        # best bid and ask
        best_bid_ask = pd.DataFrame(this_day_imi_data.best_bid_ask[orderbook_no])
        best_bid_ask_stats = calculate_best_bid_ask_statistics(
            best_bid_ask, trading_actions, tick_sizes, start_microsecond, end_microsecond
        )
        this_orderbook_stats["best_bid_ask_stats"] = best_bid_ask_stats

        # depth at best
        best_depths = pd.DataFrame(this_day_imi_data.best_depths[orderbook_no])
        best_depth_stats = calculate_best_depth_statistics(
            best_depths, trading_actions, metainfo, start_microsecond, end_microsecond
        )
        this_orderbook_stats["best_depth_stats"] = best_depth_stats

        # snapshots
        snapshots = pd.DataFrame.from_dict(
            this_day_imi_data.snapshots[orderbook_no], orient="index"
        )
        snapshots = snapshots.loc[
            int(start_microsecond * 1e-6) : int(end_microsecond * 1e-6)
        ]
        snapshot_stats = calculate_snapshot_statistics(
            snapshots, trading_actions, tick_sizes, metainfo
        )
        this_orderbook_stats["snapshot_stats"] = snapshot_stats

        # order_stats
        order_stats = pd.DataFrame.from_dict(
            this_day_imi_data.order_stats[orderbook_no], orient="index"
        )
        this_orderbook_stats["order_stats"] = calculate_order_stats(
            order_stats,
            trading_actions,
            metainfo,
            tick_sizes,
            start_microsecond,
            end_microsecond,
        )

        # message counts
        message_counts = dict(this_day_imi_data.message_counts[orderbook_no])
        message_counts["sum"] = sum(message_counts.values())
        message_counts = {"message_counts_" + key: val for key, val in message_counts.items()}
        this_orderbook_stats["message_counts"] = message_counts

        # preprocess transactions
        transactions = pd.DataFrame(this_day_imi_data.transactions[orderbook_no])
        if transactions.empty:
            continue
        transactions.set_index("timestamp", inplace=True)
        transactions = transactions.loc[start_microsecond:end_microsecond]
        if transactions.empty:
            continue
        transactions["mid"] = (
            transactions["best_ask"] + transactions["best_bid"]
        ) * 0.5
        price_decimals = 10 ** metainfo.price_decimals
        transactions[["price", "best_bid", "best_ask", "mid"]] /= price_decimals

        # trade statistics
        aggregated_statistics = calculate_effective_statistics(
            transactions, metainfo, tick_sizes
        )
        this_orderbook_stats["transaction_stats"] = aggregated_statistics

        # realized volatility
        this_orderbook_stats["realized_vola_stats"] = calculate_realized_vola_stats(
            transactions
        )

        for measure_type, measure_stats in this_orderbook_stats.items():
            for measure, value in measure_stats.items():
                metadata.loc[orderbook_no, measure] = value

    metadata["date"] = pd.Timestamp(this_day_imi_data.date)

    return metadata
