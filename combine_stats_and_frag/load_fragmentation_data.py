#!/usr/bin/env python3
"""
"""

import pandas as pd


def load_frag_data() -> pd.DataFrame:

    mapping: pd.DataFrame = load_mapping()
    frag: pd.DataFrame = load_bloomberg_data()

    frag = frag.join(mapping).sort_index()
    frag = frag.groupby("figi").transform(lambda x: x.ffill())
    frag.reset_index("figi", inplace=True)
    frag.set_index(["share_class_id_bb_global"], append=True, inplace=True)

    # calculate fragmentation index (Gresse, 2017, JFM, p. 6)
    frag["market_volume"] = frag.groupby(["date", "share_class_id_bb_global"])["volume"].sum()
    frag["market_share"] = frag["volume"] / frag["market_volume"]
    frag["market_share_squared"] = frag["market_share"] ** 2
    frag["non_fragmentation_index"] = frag.groupby(["date", "share_class_id_bb_global"])["market_share_squared"].sum()
    # frag["inverse_market_share"] = 1 / (frag["volume"] / frag["market_volume"])

    frag.reset_index("share_class_id_bb_global", inplace=True)
    del frag["market_share_squared"]
    frag.set_index("isin", append=True, inplace=True)

    # keep only six data
    frag = frag[frag["mic"].isin(["xvtx", "xswx"])]

    return frag


def load_bloomberg_data() -> pd.DataFrame:
    bloomi = pd.read_csv(f"~/data/turnover_per_venue/20191018_turnover_Bloomberg.csv", sep=";")
    bloomi = bloomi.iloc[2:].rename(columns={"Unnamed: 0": "date"}).set_index("date").stack()
    bloomi = pd.DataFrame(bloomi).reset_index().rename(columns={"level_1": "figi", 0: "volume"})
    bloomi["figi"] = bloomi["figi"].str.split(expand=True)[0]
    bloomi["date"] = pd.to_datetime(bloomi["date"], format="%d.%m.%Y")
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
            f"~/data/turnover_per_venue/turnover_{group}_20180701-20190906.csv",
        )
        mapping.append(this_frag)
    mapping: pd.DataFrame = pd.concat(mapping)
    del this_frag
    # keep only CHF
    mapping["date"] = pd.to_datetime(mapping["date"], format="%Y-%m-%d")
    mapping = mapping[mapping["ccy"] == "CHF"]
    mapping = mapping[["date", "bigi", "isin", "share_class_id_bb_global", "mic"]]
    mapping.rename(columns={"bigi": "figi"}, inplace=True)
    mapping.drop_duplicates(inplace=True)
    mapping.set_index(["date", "figi"], inplace=True)
    return mapping
