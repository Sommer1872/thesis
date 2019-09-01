#!/usr/bin/env python3
"""
"""

# standard libraries
import cProfile, pstats
from multiprocessing import Pool
import os
from pathlib import Path
import pickle
import sys
import time
from typing import Dict, List
sys.path.append("..")

# third-party packages
import pandas as pd
import numpy as np
from tqdm import tqdm

from process_messages.orderbook_stats import SingleDayIMIData


def main():

    data_path = Path.home() / "data/ITCH_market_data/binary"
    keep_dates = ["2018-07-11", "2018-07-12"]
    binary_files = get_files(data_path, keep_dates)

    start_time = time.time()
    print(f"Processing {len(binary_files)} files")
    results = load_and_process_all(binary_files)
    print(f"It took {round(time.time() - start_time, 2)} seconds to process {len(binary_files)} files")

    timestamp = str(pd.Timestamp("today"))
    with open(f"results_{timestamp}.pickle", "wb" ) as pickle_file:
        pickle.dump(results, pickle_file)


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    all_orderbook_stats, all_transaction_stats, metadata = calculate_orderbook_stats(this_day_imi_data)
    return (all_orderbook_stats, all_transaction_stats, metadata)


def load_and_process_all(binary_files: Dict[str, Path]):
    file_paths = binary_files.values()
    with Pool(processes=os.cpu_count()-1) as pool:
        results = pool.map(load_and_process_orderbook_stats, file_paths)
    return results


def calculate_orderbook_stats(this_day_imi_data: SingleDayIMIData):

    # first, nicely format metadata:
    metadata = pd.DataFrame.from_dict(this_day_imi_data.metadata, orient="index")
    string_columns = ["price_type", "isin", "currency", "group"]
    metadata[string_columns] = metadata[string_columns].apply(lambda column: column.str.decode("utf-8"))
    metadata[string_columns] = metadata[string_columns].apply(lambda column: column.str.strip())
    # keep only BlueChips
    metadata = metadata[metadata["group"] == "ACoK"]

    all_orderbook_stats = dict()
    all_transaction_stats = dict()

    # next, we process the orderbook for each stock:
    for orderbook_no in metadata.index:
        price_decimals = 10**metadata.loc[orderbook_no].price_decimals
        stats = pd.DataFrame.from_dict(this_day_imi_data.orderbook_stats[orderbook_no], orient="index")
        stats["quoted_spread"] = (stats["best_ask"] - stats["best_bid"])
        stats["mid"] = (stats["best_ask"] + stats["best_bid"]) * 0.5
        stats["relative_quoted_spread_bps"] = 100 * stats["quoted_spread"] / stats["mid"]
        stats[["quoted_spread", "mid"]] /= price_decimals
        stats[["best_ask", "best_bid"]] = stats[["best_ask", "best_bid"]] / price_decimals

        all_orderbook_stats[orderbook_no] = stats.describe()

        # effective spreads
        transactions = pd.DataFrame.from_dict(this_day_imi_data.transactions[orderbook_no], orient="index")
        transactions["mid"] = (transactions["best_ask"] + transactions["best_bid"]) * 0.5
        transactions["relative_effective_spread_bps"] = 100 * 2 * np.abs(transactions["price"] - transactions["mid"]) / transactions["mid"]
        transactions[["price", "best_bid", "best_ask", "mid"]] /= price_decimals
        transactions["effective_spread"] = 2 * np.abs(transactions["price"] - transactions["mid"])
        # spread leeway using tick sizes and an unequal join
        tick_sizes = pd.DataFrame.from_dict(this_day_imi_data.price_tick_sizes[int(metadata.loc[orderbook_no].price_tick_table_id)], orient="index")
        tick_sizes = tick_sizes.reset_index() / price_decimals
        tick_sizes.columns = ["tick_size", "price_start"]
        tick_sizes["price_end"] = tick_sizes["price_start"].shift(fill_value=np.inf)
        conditions = [(transactions.price.values >= step.price_start) &
                      (transactions.price.values < step.price_end) for step in tick_sizes[["price_start", "price_end"]].itertuples()]
        transactions["tick_size"] = np.piecewise(np.zeros(transactions.shape[0]), conditions, tick_sizes.tick_size.values)
        transactions["spread_leeway"] = transactions["effective_spread"] / transactions["tick_size"]

        all_transaction_stats[orderbook_no] = transactions.describe()

    return all_orderbook_stats, all_transaction_stats, metadata



def get_files(data_path: Path, keep_dates: List[str]) -> Dict[str, Path]:
    binary_files = dict()
    for file_path in data_path.glob("*.bin"):
        this_date = file_path.name[-14:-4].replace("_", "-")
        if this_date in keep_dates:
            binary_files[this_date] = file_path
    return binary_files


if __name__ == "__main__":
    main()
