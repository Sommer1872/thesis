#!/usr/bin/env python3
"""
"""

# standard libraries
from multiprocessing import Pool
import os
from pathlib import Path
from typing import List, Iterator, Dict

# third-party packages
import pandas as pd
from tqdm import tqdm

from calculate_statistics.calculate_all import calculate_orderbook_stats
from process_messages.process_one_day import SingleDayIMIData


def main():

    start_time = pd.Timestamp("now").strftime("%Y.%m.%d %H:%M:%S")
    print(f"Started at {start_time}")

    data_path = Path.home() / "data/ITCH_market_data/unzipped"
    pattern = "*.bin"  # *2019
    print(f"Considering files with pattern {pattern}")

    num_files = len(list(data_path.glob(pattern)))
    print(f"\nProcessing {num_files} trading days...")

    binary_file_paths = data_path.glob(pattern)
    results = load_and_process_all(binary_file_paths)

    # save to csv
    stats_path = Path("statistics/daily_liquidity")
    stats_path.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
    filepath = stats_path / f"{timestamp}_liquidity_stats.csv"
    results.to_csv(filepath, float_format="%g")
    print(f"Saved statistics to {filepath}")

    print(f"\n {5*'    '} <<<<< Done >>>>> \n")


def load_and_process_all(file_paths: Iterator[Path]) -> List[Dict]:
    with Pool(processes=os.cpu_count() - 1) as pool:
        daily_stats = list()
        parallel_processes = pool.imap_unordered(
            load_and_process_orderbook_stats, file_paths,
        )
        for single_day_statistics_output in tqdm(parallel_processes):
            daily_stats.append(single_day_statistics_output)

    # combine all days into one big dataframe
    # each row corresponds to a stock/day combination
    results = pd.concat(daily_stats, sort=False)
    del daily_stats
    results.reset_index(inplace=True)
    results.set_index("isin", inplace=True)
    results.rename(columns={"index": "orderbook_no"}, inplace=True)
    return results


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    single_day_stats = calculate_orderbook_stats(this_day_imi_data)
    return single_day_stats


if __name__ == "__main__":
    main()
