#!/usr/bin/env python3
"""
"""
from typing import Dict, List

import pandas as pd
from tqdm import tqdm


def process_daily_statistics(results: List[Dict]) -> pd.DataFrame:
    all_results = list()
    for date_stats in tqdm(results):
        metadata = date_stats["metadata"]
        this_date = date_stats["date"]
        metadata["date"] = pd.Timestamp(this_date)
        all_statistics = date_stats["all_statistics"]
        for orderbook_no, this_orderbook_stats in all_statistics.items():
            # snapshots
            snapshot_stats = this_orderbook_stats["snapshot_stats"]
            metadata.loc[orderbook_no, "mean_quoted_spread_bps"] = snapshot_stats.loc[
                "mean", "relative_quoted_spread_bps"
            ]
            metadata.loc[orderbook_no, "median_quoted_spread_bps"] = snapshot_stats.loc[
                "50%", "relative_quoted_spread_bps"
            ]
            metadata.loc[orderbook_no, "mean_depth_at_best"] = snapshot_stats.loc[
                "mean", "depth_at_best"
            ]
            metadata.loc[orderbook_no, "median_depth_at_best"] = snapshot_stats.loc[
                "50%", "depth_at_best"
            ]

            # best_bid_ask
            best_bid_ask_stats = this_orderbook_stats["best_bid_ask_stats"]
            metadata.loc[
                orderbook_no, "time_weighted_quoted_spread_bps"
            ] = best_bid_ask_stats["time_weighted_relative_quoted_spread_bps"]
            # best_depth
            best_depth_stats = this_orderbook_stats["best_depth_stats"]
            metadata.loc[
                orderbook_no, "time_weighted_average_depth"
            ] = best_depth_stats["time_weighted_average_depth"]

            # order_stats
            order_stats = this_orderbook_stats["order_stats"]
            if order_stats:
                for measure, value in order_stats.items():
                    metadata.loc[orderbook_no, measure] = value

            # realized vola
            realized_vola_stats = this_orderbook_stats.get(
                "realized_vola_stats", dict()
            )
            if realized_vola_stats:
                metadata.loc[orderbook_no, "TSRV"] = realized_vola_stats["TSRV"]
                metadata.loc[orderbook_no, "RV_slow"] = realized_vola_stats["RV_slow"]
                metadata.loc[orderbook_no, "noise_var_TSRV"] = realized_vola_stats[
                    "noise_var_TSRV"
                ]
                metadata.loc[orderbook_no, "signal_to_noise"] = realized_vola_stats[
                    "signal_to_noise"
                ]

            # transactions
            measure = "aggregated_stats"
            transaction_stats = this_orderbook_stats.get(measure, pd.DataFrame())
            if not transaction_stats.empty:
                metadata.loc[
                    orderbook_no, f"mean_eff_spread"
                ] = transaction_stats.loc["mean", "eff_spread"]
                metadata.loc[
                    orderbook_no, f"median_eff_spread"
                ] = transaction_stats.loc["50%", "eff_spread"]
                metadata.loc[
                    orderbook_no, f"mean_rel_eff_spread_bps"
                ] = transaction_stats.loc["mean", "relative_eff_spread_bps"]
                metadata.loc[
                    orderbook_no, f"median_rel_eff_spread_bps"
                ] = transaction_stats.loc["50%", "relative_eff_spread_bps"]
                metadata.loc[
                    orderbook_no, f"mean_eff_spread_leeway"
                ] = transaction_stats.loc["mean", "spread_leeway"]
                metadata.loc[
                    orderbook_no, f"median_eff_spread_leeway"
                ] = transaction_stats.loc["50%", "spread_leeway"]
                metadata.loc[
                    orderbook_no, f"mean_trade_value"
                ] = transaction_stats.loc["mean", "trade_value"]
                metadata.loc[
                    orderbook_no, f"median_trade_value"
                ] = transaction_stats.loc["50%", "trade_value"]
                metadata.loc[
                    orderbook_no, f"mean_tick_size"
                ] = transaction_stats.loc["mean", "tick_size"]
        all_results.append(metadata)

    results = pd.concat(all_results)
    del all_results
    results.reset_index(inplace=True)
    results.set_index("isin", inplace=True)
    results.rename(columns={"index": "orderbook_no"}, inplace=True)
    return results
