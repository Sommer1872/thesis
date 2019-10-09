#!/usr/bin/env python3
"""Unzips files and saves them to another location
"""
import os
import gzip
from multiprocessing import Pool
from pathlib import Path
import shutil
from typing import List

from tqdm import tqdm


def main():

    zipped_dir = Path("/data/ITCH_market_data/zipped")
    zipped_filepaths = sorted([path for path in zipped_dir.glob("*.gz")])
    results = unzip_all(zipped_filepaths)


def unzip_one_file(zipped_file_path: Path):

    unzipped_dir = zipped_file_path.parents[1] / "unzipped"
    os.makedirs(unzipped_dir, exist_ok=True)

    new_filename = zipped_file_path.stem
    new_filepath = unzipped_dir / new_filename

    if Path.exists(new_filepath):
        return f"Path {new_filepath} already exists, skipped"

    # unzip it and save new file
    with gzip.open(zipped_file_path, "rb") as f_in:
        with open(new_filepath, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # delete the .gz file
    os.remove(zipped_file_path)

    return f"File unzipped and saved to {new_filepath}"


def unzip_all(file_paths: List[Path]) -> List[str]:
    with Pool(processes=os.cpu_count() - 1) as pool:
        results = list(
            tqdm(pool.imap_unordered(unzip_one_file, file_paths),
                 total=len(file_paths)))
        return results


if __name__ == "__main__":
    main()
