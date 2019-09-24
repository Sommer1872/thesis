#!/usr/bin/env python3
"""Unzips files and saves them to another location
"""
import os
import gzip
from pathlib import Path
import shutil

from tqdm import tqdm


def main():

    home = os.path.expanduser("~")
    zipped_path = Path.home() / "data/ITCH_market_data/zipped"
    binary_path = Path.home() / "data/ITCH_market_data/binary"

    unzip_files(zipped_path, binary_path)


def unzip_files(zipped_directory: str, new_directory: str):

    os.makedirs(new_directory, exist_ok=True)

    zipped_filepaths = sorted([path for path in zipped_directory.glob("*.gz")])

    for this_filepath in tqdm(zipped_filepaths):

        new_filename = this_filepath.stem
        new_filepath = new_directory / new_filename

        if Path.exists(new_filepath):
            tqdm.write(f"Path {new_filepath} already exists, skipped")
            continue

        # unzip it and save new file
        with gzip.open(this_filepath, "rb") as f_in:
            with open(new_filepath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        tqdm.write(f"File unzipped and saved to {new_filepath}")

        # delete the .gz file
        # os.remove(this_filepath)


if __name__ == "__main__":
    main()
