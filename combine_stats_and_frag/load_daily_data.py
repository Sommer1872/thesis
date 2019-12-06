#!/usr/bin/env python3
"""
"""
from pathlib import Path

import numpy as np
import pandas as pd


def load_market_quality_statistics(filepath: Path,) -> pd.DataFrame:
    """
    """
    daily_stats = pd.read_csv(filepath)

    daily_stats["date"] = pd.to_datetime(daily_stats["date"], format="%Y-%m-%d")
    daily_stats.set_index("date", inplace=True)
    daily_stats.sort_index(inplace=True)
    # After non-equivalence dummy
    daily_stats["after_nonequivalence"] = daily_stats.index >= pd.Timestamp(
        "2019-07-01"
    )
    daily_stats["order_to_trade"] = (
        daily_stats["num_orders_total"] / daily_stats["num_transactions"]
    )
    daily_stats["num_orders_filled_percent"] = (
        daily_stats["num_orders_filled"] / daily_stats["num_orders_total"]
    )
    daily_stats["num_orders_deleted_percent"] = (
        daily_stats["num_orders_deleted"] / daily_stats["num_orders_total"]
    )
    daily_stats["message_counts_add_to_delete"] = (
        daily_stats["message_counts_add_order"]
        / daily_stats["message_counts_delete_order"]
    )
    daily_stats["log_turnover"] = np.log(daily_stats["turnover"])
    daily_stats["price_reciprocal"] = 1 / daily_stats["price_mean"]
    daily_stats["AT_proxy"] = (
        daily_stats["turnover"] / (daily_stats["message_counts_sum"]) * -1
    )  # Hendershott et al. 2017 JF p. 7

    # filter to not include delisted stocks
    last_date_avail = daily_stats.reset_index()[["date", "isin"]].groupby("isin").max()
    last_date_avail = last_date_avail.groupby("isin").max()
    last_date_avail = last_date_avail[
        last_date_avail["date"] != daily_stats.index.max()
    ]
    delisted_isins = last_date_avail.index.to_list()
    daily_stats = daily_stats[~daily_stats["isin"].isin(delisted_isins)]

    # exclude aluflexpack (because they had IPO only on June 28th)
    daily_stats.set_index("isin", append=True, inplace=True)
    daily_stats = daily_stats.drop(index="CH0453226893", level="isin")

    # exclude Alcon on 8th of April
    daily_stats.drop(index=("2019-04-08", "CH0432492467"), inplace=True)
    daily_stats.reset_index("isin", inplace=True)

    return daily_stats


def load_frag_data() -> pd.DataFrame:

    mapping: pd.DataFrame = load_mapping()
    frag: pd.DataFrame = load_bloomberg_data()

    frag = frag.join(mapping).sort_index()
    frag = frag.groupby("figi").transform(lambda x: x.ffill())
    frag.reset_index("figi", inplace=True)
    frag.set_index(["share_class_id_bb_global"], append=True, inplace=True)

    # calculate fragmentation index (similar to Gresse, 2017, JFM, p. 6)
    frag["market_volume"] = frag.groupby(["date", "share_class_id_bb_global"])[
        "volume"
    ].sum()
    frag["market_share"] = frag["volume"] / frag["market_volume"]
    frag["market_share_squared"] = frag["market_share"] ** 2
    frag["non_fragmentation_index"] = frag.groupby(
        ["date", "share_class_id_bb_global"]
    )["market_share_squared"].sum()
    # frag["inverse_market_share"] = 1 / (frag["volume"] / frag["market_volume"])

    frag.reset_index("share_class_id_bb_global", inplace=True)
    del frag["market_share_squared"]

    # keep only six data
    frag = frag[frag["mic"].isin(["xvtx", "xswx"])]

    return frag


def load_bloomberg_data() -> pd.DataFrame:
    bloomi = pd.read_csv(
        f"~/data/turnover_per_venue/20191130_turnover_Bloomberg.csv", sep=";"
    )
    bloomi = (
        bloomi.iloc[2:].rename(columns={"Unnamed: 0": "date"}).set_index("date").stack()
    )
    bloomi = (
        pd.DataFrame(bloomi)
        .reset_index()
        .rename(columns={"level_1": "figi", 0: "volume"})
    )
    bloomi["figi"] = bloomi["figi"].str.split(expand=True)[0]
    bloomi["date"] = pd.to_datetime(bloomi["date"], format="%d.%m.%y")
    bloomi.drop_duplicates(inplace=True)
    bloomi.set_index(["date", "figi"], inplace=True)
    bloomi["volume"] = bloomi["volume"].astype(float)
    return bloomi


def load_mapping() -> pd.DataFrame:
    groups = ["ACoK", "ABck"]  # ACoK = Blue Chips, ABck = Mid-/Small-Caps
    # load data
    mapping = list()
    for group in groups:
        this_frag = pd.read_csv(
            f"~/data/turnover_per_venue/turnover_{group}_20180701-20190906.csv"
        )
        mapping.append(this_frag)
    mapping: pd.DataFrame = pd.concat(mapping)
    del this_frag
    # keep only CHF
    mapping["date"] = pd.to_datetime(mapping["date"], format="%Y-%m-%d")
    mapping = mapping[mapping["ccy"] == "CHF"]
    mapping = mapping[
        ["date", "bigi", "isin", "share_class_id_bb_global", "mic", "bb_ticker"]
    ]
    mapping.rename(columns={"bigi": "figi"}, inplace=True)
    mapping.drop_duplicates(inplace=True)
    mapping.set_index(["date", "figi"], inplace=True)
    return mapping
