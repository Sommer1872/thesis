#!/usr/bin/env python3
"""
"""
import numpy as np
import pandas as pd

def calculate_best_bid_ask_statistics(best_bid_ask: pd.DataFrame, metainfo: pd.Series,
    start_microsecond: int, end_microsecond: int) -> pd.DataFrame:

    price_decimals = 10 ** metainfo.price_decimals

    best_bid_ask.drop_duplicates(subset=["timestamp", "book_side"], inplace=True, keep="last")
    # show bids/asks side by side
    best_bid_ask = best_bid_ask.pivot(index="timestamp", columns="book_side", values="new_best_price")
    best_bid_ask.columns = [col.decode("utf-8") for col in best_bid_ask.columns]
    del best_bid_ask[" "]
    # if we do not have updates on both book sides
    if not best_bid_ask.shape[1] == 2:
        return np.nan
    # get prices in chf
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
    # filter based on start / end time and get prices in chf
    best_bid_ask = best_bid_ask.loc[start_microsecond:end_microsecond]

    time_weighted_quoted_spread = np.sum(best_bid_ask["quoted_spread"] * best_bid_ask["time_validity"]) / best_bid_ask["time_validity"].sum()
    # best_bid_ask["mid_price"] = (best_bid_ask["b"] + best_bid_ask["s"]) * 0.5
    return time_weighted_quoted_spread
