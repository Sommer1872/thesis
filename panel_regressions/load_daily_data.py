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
    daily_stats["min_tick_size"] = daily_stats["tick_size_mean"]
    daily_stats.drop(columns="tick_size_mean", inplace=True)
    daily_stats["min_tick_size_relative"] = daily_stats["min_tick_size"] / daily_stats["price_mean"]
    daily_stats["price_reciprocal"] = 1 / daily_stats["price_mean"]
    daily_stats["price_log"] = np.log(daily_stats["price_mean"])
    daily_stats["AT_proxy"] = (
        (daily_stats["turnover"] / 100) / (daily_stats["message_counts_sum"]) * -1
    )  # Hendershott et al. 2011 JF p. 7

    print(f"Initial number of stocks {daily_stats['isin'].nunique()}\n")

    print("Filter to not include delisted stocks")
    last_date_avail = daily_stats.reset_index()[["date", "isin"]].groupby("isin").max()
    last_date_avail = last_date_avail.groupby("isin").max()
    last_date_avail = last_date_avail[
        last_date_avail["date"] != daily_stats.index.max()
    ]
    delisted_isins = last_date_avail.index.to_list()
    daily_stats = daily_stats[~daily_stats["isin"].isin(delisted_isins)]
    print(f"Num remaining stocks {daily_stats['isin'].nunique()}\n")

    print("Exclude all stocks that had an IPO later than January 1st 2019")
    daily_stats.set_index("isin", append=True, inplace=True)
    first_traded = daily_stats.reset_index().groupby("isin")["date"].min()
    first_traded = first_traded.reset_index()
    bad_isins = first_traded.loc[
        first_traded["date"] >= pd.Timestamp("20190101"), "isin"
    ]
    for isin in bad_isins:
        daily_stats.drop(index=isin, level="isin", inplace=True)

    # print("also dropping Panalpina, because it was taken-over in August, but continued trading")
    # daily_stats.drop(index="CH0002168083", level="isin", inplace=True)

    # exclude entries with no messages sent
    daily_stats.dropna(subset=["message_counts_sum"], inplace=True)

    daily_stats.reset_index("isin", inplace=True)
    print(f"Num remaining stocks {daily_stats['isin'].nunique()}\n")

    # join VSMI
    vsmi = load_vsmi()
    daily_stats = daily_stats.join(vsmi["VSMI"], how="left", on="date")

    return daily_stats


def load_vsmi() -> pd.DataFrame:
    """
    """
    vsmi = pd.read_csv("../statistics/h_vsmi_30.csv", sep=";")
    vsmi.columns = vsmi.columns.str.lower()
    vsmi.rename(columns={"indexvalue": "VSMI"}, inplace=True)
    vsmi["date"] = pd.to_datetime(vsmi["date"], format="%d.%m.%Y")
    vsmi.set_index("date", inplace=True)
    return vsmi


def load_copustat():
    path = Path("/Users/simon/data/turnover_per_venue/20200118_compustat.csv")
    variables = dict(
        isin="isin",
        datadate="date",
        cshoc="shares_outstanding",
        cshtrd="trading_volume",
        prccd="price_close",
        exchg="exchange_code",
        #     prchd="price_high",
        #     prcld="price_low",
    )
    compu = pd.read_csv(path)
    compu.rename(columns=variables, inplace=True)
    compu.drop(
        columns=[col for col in compu.columns if col not in variables.values()],
        inplace=True,
    )
    compu = compu[compu["exchange_code"] == 151]
    compu["date"] = pd.to_datetime(compu["date"], format="%Y%m%d")
    compu.set_index(["date", "isin"], inplace=True)
    return compu


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
    frag["lit_frag"] = frag.groupby(
        ["date", "share_class_id_bb_global"]
    )["market_share_squared"].sum() ** -1
    # frag["inverse_market_share"] = 1 / (frag["volume"] / frag["market_volume"])

    frag.reset_index("share_class_id_bb_global", inplace=True)
    del frag["market_share_squared"]

    # keep only six data
    frag = frag[frag["mic"].isin(["xvtx", "xswx"])]

    return frag


def load_bloomberg_data() -> pd.DataFrame:
    bloomi = pd.read_csv(
        f"~/data/turnover_per_venue/20191231_turnover_bloomberg.csv", sep=";"
    )
    bloomi = (
        bloomi.iloc[1:].rename(columns={"Unnamed: 0": "date"}).set_index("date").stack()
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
