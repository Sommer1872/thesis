#!/usr/bin/env python3
"""
"""
from typing import Dict
import numpy as np
import pandas as pd


def calculate_best_bid_ask_statistics(
    best_bid_ask: pd.DataFrame,
    trading_actions: pd.DataFrame,
    tick_sizes: pd.DataFrame,
    start_microsecond: int,
    end_microsecond: int,
) -> Dict[str, float]:

    best_bid_ask.drop_duplicates(
        subset=["timestamp", "book_side"], inplace=True, keep="last"
    )
    # exclude market orders from our calculations
    best_bid_ask = best_bid_ask[best_bid_ask["new_best_price"] != 2147483647]
    # show bids/asks side by side
    best_bid_ask = best_bid_ask.pivot(
        index="timestamp", columns="book_side", values="new_best_price"
    )
    best_bid_ask.columns = [col.decode("utf-8") for col in best_bid_ask.columns]
    del best_bid_ask[" "]
    # if we do not have updates on both book sides, return empty result
    if not best_bid_ask.shape[1] == 2:
        return empty_result()

    # remember cases when there are not orders on both sides of the order book
    missing = best_bid_ask.isna().all(axis=1)
    best_bid_ask.fillna(method="ffill", inplace=True)
    # in missing cases from above, we set nan's to bid/ask/mid
    best_bid_ask[missing] = np.nan
    best_bid_ask["quoted_spread"] = best_bid_ask["S"] - best_bid_ask["B"]
    best_bid_ask["mid"] = (best_bid_ask["S"] + best_bid_ask["B"]) * 0.5
    best_bid_ask["relative_quoted_spread_bps"] = (
        best_bid_ask["quoted_spread"] / best_bid_ask["mid"]
    ) * 100
    best_bid_ask.reset_index(inplace=True)
    best_bid_ask["time_validity"] = (
        best_bid_ask["timestamp"].shift(-1) - best_bid_ask["timestamp"]
    )
    best_bid_ask.set_index("timestamp", drop=True, inplace=True)
    # filter based on start / end time and get prices in chf
    best_bid_ask = best_bid_ask.loc[start_microsecond:end_microsecond]
    # filter based on trading_actions
    if not trading_actions.empty:
        for _, event in trading_actions.iterrows():
            best_bid_ask = best_bid_ask[
                (best_bid_ask.index < event.timestamp)
                | (best_bid_ask.index > event.until)
            ]
    # if there are still strange values, we remove them
    best_bid_ask = best_bid_ask[best_bid_ask["quoted_spread"] >= 0]

    # for side in ["B", "S"]:
    #     prices = best_bid_ask[side]
    #     # unequal join
    #     conditions = [
    #         (prices.values >= step.price_start) & (prices.values < step.price_end)
    #         for step in tick_sizes[["price_start", "price_end"]].itertuples()
    #     ]
    #     best_bid_ask[f"tick_size_{side}"] = np.piecewise(
    #         np.zeros(prices.shape[0]), conditions, tick_sizes.tick_size.values
    #     )

    # best_bid_ask["distance_to_B"] = (
    #     best_bid_ask["mid"] - best_bid_ask["B"]
    # ) / best_bid_ask["tick_size_B"]
    # best_bid_ask["distance_to_S"] = (
    #     best_bid_ask["S"] - best_bid_ask["mid"]
    # ) / best_bid_ask["tick_size_S"]
    # best_bid_ask["spread_leeway"] = round(
    #     best_bid_ask["distance_to_B"] + best_bid_ask["distance_to_S"] - 1
    # )

    total_time = best_bid_ask["time_validity"].sum()
    if total_time > 0:
        # quoted_spread_leeway_time_weighted = (
        #     np.sum(best_bid_ask["spread_leeway"] * best_bid_ask["time_validity"])
        #     / total_time
        # )
        quoted_rel_spread_bps_time_weighted = (
            np.sum(
                best_bid_ask["relative_quoted_spread_bps"]
                * best_bid_ask["time_validity"]
            )
            / total_time
        )
        return {
            # "quoted_spread_leeway_time_weighted": quoted_spread_leeway_time_weighted,
            "quoted_rel_spread_bps_time_weighted": quoted_rel_spread_bps_time_weighted,
        }
    else:
        return empty_result()


def empty_result():
    return {
        "quoted_spread_leeway_time_weighted": np.nan,
        "quoted_rel_spread_bps_time_weighted": np.nan,
    }
