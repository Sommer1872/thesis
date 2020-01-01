#!/usr/bin/env python3
"""
"""
# standard libraries
from multiprocessing import Pool
import os
from pathlib import Path
from typing import Any, Callable, Iterator

# third-party packages
import pandas as pd
from tqdm import tqdm

from calculate_statistics.calculate_all import calculate_orderbook_stats
from process_messages.process_one_day import SingleDayIMIData
from survival.compute import compute_survival


def main():

    start_time = pd.Timestamp("now").strftime("%Y.%m.%d %H:%M:%S")
    print(f"Started at {start_time}")

    data_path = Path.home() / "data/ITCH_market_data/unzipped"
    pattern = ".bin"  # *2019*
    print(f"Considering files with pattern {pattern}")

    num_files = len(list(data_path.glob(pattern)))
    print(f"\nProcessing {num_files} trading days...")

    binary_file_paths = data_path.glob(pattern)
    orders = process_parallel(binary_file_paths, load_and_process_orderbook_stats)
    orders = pd.concat(orders, sort=False)

    model_path = Path("statistics/models")
    model_path.mkdir(parents=True, exist_ok=True)

    print("Estimating survival models...")
    process_parallel(orders.groupby("yearmonth"), compute_survival)

    # # save to csv
    # timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H-%M-%S")
    # filepath = stats_path / f"{timestamp}_survival_times.zip"
    # results.to_csv(filepath, float_format="%g", compression="zip")
    # print(f"Saved statistics to {filepath}")

    print(f"\n {5*'    '} <<<<< Done >>>>> \n")


def process_parallel(inputs: Iterator[Any], function: Callable) -> pd.DataFrame:
    """Applies the function to all inputs, in parallel
    """
    with Pool(processes=os.cpu_count() - 1) as pool:
        results = list()
        parallel_processes = pool.imap_unordered(function, inputs,)
        for output in tqdm(parallel_processes):
            results.append(output)
    return results


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    single_day_stats = calculate_orderbook_stats(this_day_imi_data)
    return single_day_stats


if __name__ == "__main__":
    main()
