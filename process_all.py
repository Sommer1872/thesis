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

# third-party packages
import pandas as pd
from tqdm import tqdm

from process_messages.orderbook_stats import SingleDayIMIData
from calculate_statistics.calculate import calculate_orderbook_stats


def main():

    data_path = Path.home() / "data/ITCH_market_data/binary"
    keep_dates = ["2018-07-11", "2018-07-12"]
    binary_files = get_files(data_path, keep_dates)

    start_time = time.time()
    print(f"Processing {len(binary_files)} dates (1 file per date)")
    results = load_and_process_all(binary_files)
    print(
        f"It took {round(time.time() - start_time, 2)} seconds to process {len(binary_files)} dates"
    )

    timestamp = str(pd.Timestamp("today"))
    with open(f"results_{timestamp}.pickle", "wb") as pickle_file:
        pickle.dump(results, pickle_file)


def load_and_process_orderbook_stats(file_path: Path):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    results = calculate_orderbook_stats(this_day_imi_data)
    return results


def load_and_process_all(binary_files: Dict[str, Path]):
    file_paths = binary_files.values()
    results = list()
    with Pool(processes=os.cpu_count() - 1) as pool:
        for result in tqdm(
            pool.imap_unordered(load_and_process_orderbook_stats, file_paths)
        ):
            results.append(result)
    return results


def get_files(data_path: Path, keep_dates: List[str]) -> Dict[str, Path]:
    binary_files = dict()
    for file_path in data_path.glob("*.bin"):
        this_date = file_path.name[-14:-4].replace("_", "-")
        if this_date in keep_dates:
            binary_files[this_date] = file_path
    return binary_files


if __name__ == "__main__":
    main()
