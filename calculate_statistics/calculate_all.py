#!/usr/bin/env python3
"""
"""
import pandas as pd
import numpy as np

from .order_stats import calculate_order_stats


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

    all_statistics = list()
    # next, we calculate various statistics for each stock:
    for orderbook_no in metadata.index:

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

        # order_stats
        order_stats = pd.DataFrame.from_dict(
            this_day_imi_data.order_stats[orderbook_no], orient="index"
        )
        close_to_best = calculate_order_stats(
            order_stats,
            trading_actions,
            metainfo,
            tick_sizes,
            start_microsecond,
            end_microsecond,
        )
        close_to_best["orderbook_no"] = orderbook_no
        close_to_best.set_index("orderbook_no", inplace=True)
        all_statistics.append(close_to_best)

    survival_times = pd.concat(all_statistics, sort=False)

    # add date as a column
    survival_times["date"] = pd.Timestamp(this_day_imi_data.date)

    # isin instead of orderbook_no's
    survival_times = survival_times.join(metadata["isin"])
    survival_times.set_index("isin", inplace=True)

    return survival_times
