#!/usr/bin/env python3
"""
"""
from typing import Tuple

from lifelines import KaplanMeierFitter
from lifelines.statistics import (
    logrank_test,
    survival_difference_at_fixed_point_in_time_test,
)
from lifelines.utils import restricted_mean_survival_time
import numpy as np
import pandas as pd


def compute_survival(group: Tuple[str, pd.DataFrame]) -> pd.DataFrame:
    """
    """
    isin: str
    orders: pd.DataFrame
    isin, orders = group

    orders.set_index("date", inplace=True)
    orders.sort_index(inplace=True)

    stats = dict()
    survival_functions = list()

    for measure in ("time_to_fill", "time_to_removal"):

        models = dict()

        series = orders[measure].dropna()
        upper_limit = series.quantile(0.9)
        timeline = np.linspace(0, upper_limit, num=50)

        non_equivalence_date = pd.Timestamp(2019, 7, 1)
        before = series.loc[: non_equivalence_date - pd.Timedelta("1 days")]
        after = series.loc[non_equivalence_date:]

        if before.empty or after.empty:
            return pd.DataFrame()

        for timespan_name, timespan in {"before": before, "after": after}.items():

            event_observed = timespan <= upper_limit

            model = KaplanMeierFitter()
            model.fit(
                durations=timespan,
                event_observed=event_observed,
                timeline=timeline,
                label=f"{timespan_name} {measure}",
            )
            models[timespan_name] = model
            survival_functions.append(model.survival_function_)

        this_stats = stats[measure] = dict()
        durations_before = models["before"].durations
        durations_after = models["after"].durations

        median_before = models["before"].median_survival_time_
        upper_limit = min(
            models["before"].survival_function_.index.max(),
            models["after"].survival_function_.index.max(),
        )

        # calculate and keep track of stats
        this_stats["num_orders_before"] = before.shape[0]
        this_stats["num_orders_after"] = after.shape[0]

        this_stats["median_survival_before"] = median_before
        this_stats["median_survival_after"] = models["after"].median_survival_time_

        try:
            test = logrank_test(durations_before, durations_after)
            this_stats["logrank_test_pval"] = test.summary.p[0]
        except AssertionError:
            this_stats["logrank_test_pval"] = np.nan

        test = survival_difference_at_fixed_point_in_time_test(
            median_before, durations_before, durations_after
        )
        this_stats["survival_diff_pval"] = test.summary.p[0]

        rmst_before = restricted_mean_survival_time(models["before"], t=upper_limit)
        rmst_after = restricted_mean_survival_time(models["after"], t=upper_limit)
        this_stats["rmst_diff"] = rmst_after - rmst_before
        this_stats["rmst_change_pct"] = ((rmst_after / rmst_before) - 1) * 100

    survival_functions = pd.concat(survival_functions, sort=True).round(3)
    survival_functions.index = np.round(survival_functions.index.values, 3)
    survival_functions["isin"] = isin

    stats = pd.DataFrame(stats)
    stats.index.name = "measure"
    stats["isin"] = isin
    stats = stats.round(3)

    outputs = pd.concat([survival_functions, stats], sort=False)

    return outputs
