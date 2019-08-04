#!/usr/bin/env python3
"""
"""

# standard libraries
import cProfile, pstats
from multiprocessing import Pool
import os
from pathlib import Path
import time
from typing import Dict, List

# third-party packages
import pandas as pd
from tqdm import tqdm

from data_handling.new_preprocess import SingleDayIMIData


def main():

    data_path = Path.home() / "data/ITCH_market_data/binary"
    keep_dates = ["2018-07-11", "2018-07-12"]
    binary_files = get_files(data_path, keep_dates)

    profile_one(file_path = binary_files["2018-07-11"])

    # start_time = time.time()
    # load_and_process_all(binary_files)
    # print(f"It took {round(time.time() - start_time, 2)} seconds to process {len(binary_files)} files")


def profile_one(file_path):
    # Start profiler
    pr = cProfile.Profile()
    pr.enable()
    # process single file
    load_and_process_one(file_path)
    # stop and print stats
    pr.disable()
    sortby = 'tottime'
    ps = pstats.Stats(pr).sort_stats(sortby)
    ps.print_stats()


def get_files(data_path: str, keep_dates: List[str]) -> Dict[str, str]:
    binary_files = dict()
    for file in os.listdir(data_path):
        if file.endswith(".bin"):
            this_date = file[-14:-4].replace("_", "-")
            if this_date in keep_dates:
                binary_files[this_date] = data_path / Path(file)
    return binary_files


def load_and_process_all(binary_files: Dict[str, str]):
    file_paths = binary_files.values()
    with Pool(processes=os.cpu_count()-2) as pool:
        pool.map(load_and_process_one, file_paths)


def load_and_process_one(file_path: str):
    this_day_imi_data = SingleDayIMIData(file_path)
    this_day_imi_data.process_messages()
    return this_day_imi_data


if __name__ == "__main__":
    main()
