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
from tqdm import tqdm

from calculate_statistics.calculate_all import calculate_orderbook_stats
from process_messages.orderbook_stats import SingleDayIMIData
from results.process_results import process_results


def main():

    data_path = Path.home() / "data/ITCH_market_data/unzipped"
    binary_file_paths = data_path.glob("*.bin")

    num_files = len(list(data_path.glob("*.bin")))
    print(f"\nProcessing {num_files} trading days...")
    results = load_and_process_all(binary_file_paths)

    print(f"\nProcessing results...")
    results = process_results(results)

    timestamp = str(pd.Timestamp("today").ceil("1s")).replace(":", "-")
    os.makedirs("results", exist_ok=True)

    results.to_csv(f"results/{timestamp}", float_format="%g")

    print(f"\n {5*'    '} <<<<< Done >>>>> \n")


def load_and_process_all(file_paths: Iterator[Path]) -> List[tuple]:
    with Pool(processes=os.cpu_count() - 1) as pool:
        results = list(tqdm(pool.imap_unordered(load_and_process_orderbook_stats, file_paths)))
    return results


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    results = calculate_orderbook_stats(this_day_imi_data)
    return results


if __name__ == "__main__":
    main()
