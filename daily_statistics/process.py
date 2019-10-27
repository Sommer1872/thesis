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
        measure_types = [
            "snapshot_stats",
            "best_bid_ask_stats",
            "best_depth_stats",
            "order_stats",
            "realized_vola_stats",
            "transaction_stats",
        ]
        for measure_type in measure_types:
            measure_stats = this_orderbook_stats.get(measure_type, dict())
            if measure_stats:
                for measure, value in measure_stats.items():
                    metadata.loc[orderbook_no, measure] = value

    return metadata
