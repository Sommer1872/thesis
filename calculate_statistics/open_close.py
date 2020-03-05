#!/usr/bin/env python3
"""

NOT FINISHED YET

"""
from typing import Dict

import numpy as np
import pandas as pd


def retrieve_open_and_close(
    open_close: pd.DataFrame,
    start_trading: int,
    stop_trading: int,
    metainfo: pd.DataFrame,
) -> Dict[str, float]:

    stats = dict()
    open_close.set_index("timestamp", inplace=True)
    for price_type, auction_time in [
        ("price_opening", start_trading),
        ("price_closing", stop_trading),
    ]:
        try:
            auction_prices = open_close.loc[auction_time, "price"].value_counts()
            if auction_prices.empty:
                price = np.nan
            else:
                price = auction_prices.index[0]
        except KeyError:
            price = np.nan

        stats[price_type] = price / 10**metainfo.price_decimals

    return stats
