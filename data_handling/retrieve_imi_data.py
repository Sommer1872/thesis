#!/usr/bin/env python3
"""Retrieves historical data from SIX IMI data feed
"""
import os
import getpass
from pathlib import Path
import requests
import shutil

import pandas as pd
from tqdm import tqdm


def main():

    start_date = pd.Timestamp(2019, 6, 11)
    end_date = pd.Timestamp(2019, 6, 11)
    username = "simon.sommer@student.unisg.ch"

    # retrieve and download all IMI files between start_date and end_date
    get_IMI_data(start_date, end_date, username)


def get_IMI_data(start_date: pd.Timestamp, end_date: pd.Timestamp, username: str):
    """
    """
    print(f"\nUser: {username}")
    password = getpass.getpass('Please enter password here and confirm with <ENTER>')

    weekdays = get_weekday_dates(start_date, end_date)

    home = os.path.expanduser("~")
    data_path = f"{home}/data/ITCH_market_data/zipped"
    os.makedirs(data_path, exist_ok=True)

    for timestamp in tqdm(weekdays):

        month = str(timestamp.month).rjust(2, '0')
        filename = f"ITCHTV-P01_{str(timestamp.date()).replace('-', '_')}.bin.gz"
        filepath = Path(os.path.join(data_path, filename))

        if filepath.with_suffix('').exists():
            continue

        # download the file
        curl_data(filename, filepath, month, username=username, password=password)

        tqdm.write(f"File saved to:\n{filepath}\n")


def curl_data(filename: str, filepath: Path, month: str, username: str, password: str):
    """
    """
    url = f"https://www.exfeed.com/client_area/download/imi/{month}/{filename}"

    response = requests.get(url, auth=(username, password), stream=True)

    if response.status_code == 200:
        with open(filepath, 'wb') as file_handler:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, file_handler)

    else:
        raise ValueError("response.status.code was not == 200")


def get_weekday_dates(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DatetimeIndex:
    """
    :return: a pandas index (iterable) of datetimes that contains dates from start_date till end_date
    """
    all_dates = pd.date_range(start=start_date, end=end_date)
    is_weekday = all_dates.weekday <= 4

    weekdays = all_dates[is_weekday]

    return weekdays


if __name__ == "__main__":
    main()
