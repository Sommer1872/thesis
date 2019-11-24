#!/usr/bin/env python3
"""
"""
from typing import Dict, Any

import pandas as pd


def process_daily_statistics(date_stats: Dict[str, Any]) -> pd.DataFrame:
    """
    """
    metadata = date_stats["metadata"]
    this_date = date_stats["date"]
    metadata["date"] = pd.Timestamp(this_date)
    all_statistics = date_stats["all_statistics"]

    for orderbook_no, this_orderbook_stats in all_statistics.items():
        for measure_type, measure_stats in this_orderbook_stats.items():
            if measure_stats:
                for measure, value in measure_stats.items():
                    metadata.loc[orderbook_no, measure] = value

    return metadata
