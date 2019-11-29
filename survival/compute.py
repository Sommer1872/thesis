#!/usr/bin/env python3
"""
"""
from typing import Tuple
from lifelines import KaplanMeierFitter
import pandas as pd


def compute_survival(group: Tuple[str, pd.DataFrame]) -> pd.DataFrame:
    """
    """
    isin: str
    orders: pd.DataFrame
    isin, orders = group

    orders.set_index("date", inplace=True)
    orders.sort_index(inplace=True)
    non_equivalence_date = pd.Timestamp(2019, 7, 1)

    before = orders.loc[:non_equivalence_date - pd.Timedelta("1 days")]
    after = orders.loc[non_equivalence_date:]
    measures = ["time_to_removal", "time_to_fill"]
    for fraction in (before, after):
        for measure in