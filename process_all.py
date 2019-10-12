#!/usr/bin/env python3
"""
"""

# standard libraries
from multiprocessing import Pool
import os
from pathlib import Path
import pickle
import sys
import time
from typing import Dict, List, Iterator

# third-party packages
import pandas as pd
import pickle
from tqdm import tqdm

from calculate_statistics.calculate_all import calculate_orderbook_stats
from process_messages.orderbook_stats import SingleDayIMIData
from daily_statistics.process import process_daily_statistics


def main():

    data_path = Path.home() / "data/ITCH_market_data/unzipped"
    pattern = "*2019*.bin"
    print(f"Considering files with pattern {pattern}")
    binary_file_paths = data_path.glob(pattern)

    num_files = len(list(data_path.glob(pattern)))
    print(f"\nProcessing {num_files} trading days...")
    daily_stats = load_and_process_all(binary_file_paths)

    print(f"\nProcessing results...")
    # Save to pickle just to be safe
    stats_path = Path("daily_statistics/stats")
    stats_path.mkdir(exist_ok=True)
    timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
    filepath = stats_path / f"{timestamp}_daily_stats.pickle"
    with open(filepath, "wb") as output_file:
        pickle.dump(daily_stats, output_file, protocol=pickle.HIGHEST_PROTOCOL)

    # further processing
    results = process_daily_statistics(daily_stats)

    timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
    filepath = stats_path / f"{timestamp}_daily_stats.csv"
    results.to_csv(filepath, float_format="%g")
    print(f"Saved daily statistics file to {filepath}")

    print(f"\n {5*'    '} <<<<< Done >>>>> \n")


def load_and_process_all(file_paths: Iterator[Path]) -> List[tuple]:
    with Pool(processes=os.cpu_count() - 1) as pool:
        results = list()
        parallel_processes = pool.imap_unordered(load_and_process_orderbook_stats, file_paths)
        for result in tqdm(parallel_processes):
            results.append(result)
    return results


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    results = calculate_orderbook_stats(this_day_imi_data)
    return results


if __name__ == "__main__":
    main()
