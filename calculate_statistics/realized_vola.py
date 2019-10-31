#!/usr/bin/env python3
"""
"""
from typing import Dict

import numpy as np
import pandas as pd


def calculate_realized_vola_stats(transactions: pd.DataFrame) -> Dict[str, float]:

    grouped = transactions.groupby(["timestamp", "price", "aggressor", "mid"])
    transactions = grouped[["price", "mid"]].mean()
    transactions.reset_index(["mid", "price", "aggressor"], drop=True, inplace=True)

    # change the index from microseconds int to pd.DatetimeIndex
    transactions.set_index(
        pd.Timestamp("2019-01-01")  # the date doesn't matter here
        + pd.to_timedelta(transactions.index, unit="us"),
        inplace=True,
    )

    log_mid_prices = np.log(transactions["mid"]) * 100

    # high sampling frequency (short timescale)
    high_sampling_freq = pd.Timedelta(5, "seconds")

    # low sampling frequency (long timescale)
    low_sampling_freq = pd.Timedelta(5, "minutes")

    # RV slow
    resampled_low_freq = log_mid_prices.resample(
        low_sampling_freq, base=0, label="right", closed="right"
    ).last()
    resampled_low_freq.dropna(inplace=True)
    resampled_low_freq = pd.DataFrame(resampled_low_freq)
    log_returns = resampled_low_freq["mid"] - resampled_low_freq["mid"].shift(1)
    RV_slow = np.sum(np.square(log_returns))

    # TSRV
    if log_mid_prices.shape[0] > 200:
        TSRV_stats = compute_TSRV(log_mid_prices, high_sampling_freq, low_sampling_freq)
    else:
        TSRV_stats = dict()

    # combine the slow and fast statistics
    stats = {"RV_slow": RV_slow, **TSRV_stats}

    return stats


def compute_TSRV(
    log_mid_prices: pd.Series,
    high_sampling_freq: pd.Timedelta,
    low_sampling_freq: pd.Timedelta,
) -> Dict[str, float]:
    # keeping the last trade per high sampling frequency
    resampled_high_freq = log_mid_prices.resample(
        high_sampling_freq, label="right", closed="right"
    ).last()
    # dealing with NaNs is important for return calculation below
    resampled_high_freq.dropna(inplace=True)
    num_obs = resampled_high_freq.shape[0]
    if num_obs == 0:
        return dict()

    log_returns = resampled_high_freq - resampled_high_freq.shift(1)

    # Realized volatility for high frequency
    RV_all = np.sum(np.square(log_returns))
    noise_var_TSRV = RV_all / (2 * num_obs)

    # TSRV
    frac = high_sampling_freq / pd.Timedelta(1, "minute")
    offsets = [
        frac * multiple
        for multiple in range(int(low_sampling_freq / high_sampling_freq))
    ]
    all_realized_volas = list()
    all_ns = list()
    for offset in offsets:
        resampled_low_freq = resampled_high_freq.resample(
            low_sampling_freq, base=offset, label="right", closed="right"
        ).last()
        resampled_low_freq.dropna(inplace=True)
        resampled_low_freq = pd.DataFrame(resampled_low_freq)

        log_returns = resampled_low_freq["mid"] - resampled_low_freq["mid"].shift(1)
        RV_offset = np.sum(np.square(log_returns))
        all_realized_volas.append(RV_offset)
        all_ns.append(log_returns.shape[0])

    # average of low freq estimates
    RV_average = np.mean(all_realized_volas)
    n_bar = np.mean(all_ns)

    # combine both for two-scales RV
    TSRV = RV_average - (n_bar / num_obs) * RV_all
    # small sample adjustment
    TSRV = 1 / (1 - n_bar / num_obs) * TSRV  # / sum(delta))

    Delta = 1 / num_obs
    denom = TSRV * Delta + 2 * noise_var_TSRV
    if denom > 0:
        signal_to_noise = 2 * noise_var_TSRV / denom
    else:
        signal_to_noise = np.nan

    TSRV_stats = {
        "TSRV": TSRV,
        "noise_var_TSRV": noise_var_TSRV,
        "signal_to_noise": signal_to_noise,
    }
    return TSRV_stats
