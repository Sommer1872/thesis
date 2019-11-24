#!/usr/bin/env python3
"""
"""

# standard libraries
from multiprocessing import Pool
import os
from pathlib import Path
import pickle
from typing import List, Iterator, Dict

# third-party packages
import pandas as pd
from tqdm import tqdm

from calculate_statistics.calculate_all import calculate_orderbook_stats
from process_messages.process_one_day import SingleDayIMIData
from daily_statistics.process import process_daily_statistics


def main():

    start_time = pd.Timestamp("now").strftime("%Y.%m.%d %H:%M:%S")
    print(f"Started at {start_time}")

    data_path = Path.home() / "data/ITCH_market_data/unzipped"
    pattern = "*2019*.bin"
    print(f"Considering files with pattern {pattern}")
    binary_file_paths = data_path.glob(pattern)

    num_files = len(list(data_path.glob(pattern)))
    print(f"\nProcessing {num_files} trading days...")
    daily_stats = load_and_process_all(binary_file_paths)

    # further processing
    print(f"\nProcessing results...")
    results = combine_daily_statistics(daily_stats)

    # save results to csv
    stats_path = Path("daily_statistics/stats")
    stats_path.mkdir(exist_ok=True)
    timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
    filepath = stats_path / f"{timestamp}_daily_stats.csv"
    results.to_csv(filepath, float_format="%g")
    print(f"Saved daily statistics file to {filepath}")

    print(f"\n {5*'    '} <<<<< Done >>>>> \n")


def load_and_process_all(file_paths: Iterator[Path]) -> List[Dict]:
    with Pool(processes=os.cpu_count() - 1) as pool:
        daily_stats = list()
        parallel_processes = pool.imap_unordered(
            load_and_process_orderbook_stats, file_paths,
        )
        for single_day_statistics_output in tqdm(parallel_processes):
            daily_stats.append(single_day_statistics_output)
    return daily_stats


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    single_day_stats = calculate_orderbook_stats(this_day_imi_data)
    return single_day_stats


def combine_daily_statistics(daily_stats: List[Dict]):
    with Pool(processes=os.cpu_count() - 1) as pool:
        all_results = list()
        parallel_processes = pool.imap_unordered(process_daily_statistics, daily_stats)
        for daily_result in tqdm(parallel_processes):
            all_results.append(daily_result)

    # combine everything into one big dataframe
    results = pd.concat(all_results, sort=False)
    del all_results
    results.reset_index(inplace=True)
    results.set_index("isin", inplace=True)
    results.rename(columns={"index": "orderbook_no"}, inplace=True)
    return results


if __name__ == "__main__":
    main()
