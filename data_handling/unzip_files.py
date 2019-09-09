#!/usr/bin/env python3
"""Retrieves historical data from SIX IMI data feed
"""
import os
import gzip
from pathlib import Path
import shutil

from tqdm import tqdm


def main():

    home = os.path.expanduser("~")
    zipped_path = Path(f"{home}/data/ITCH_market_data/zipped")
    binary_path = Path(f"{home}/data/ITCH_market_data/binary")

    unzip_files(zipped_path, binary_path)


def unzip_files(zipped_directory: str, new_directory: str):

    os.makedirs(new_directory, exist_ok=True)

    zipped_filenames = sorted([path.name for path in zipped_directory.glob("*.gz")])

    for this_filename in tqdm(zipped_filenames):

        this_filepath = zipped_directory / this_filename
        new_filename = this_filename.replace(".gz", "")
        new_filepath = new_directory / new_filename

        if Path.exists(new_filepath):
            tqdm.write(f"Path {new_filepath} already exists, skipped")
            continue

        # unzip it and save binary file
        with gzip.open(this_filepath, "rb") as f_in:
            with open(new_filepath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        tqdm.write(f"File unzipped and saved to {new_filepath}")

        # delete the .gz file
        # os.remove(this_filepath)


if __name__ == "__main__":
    main()
