#!/usr/bin/env python3
"""
"""
from collections import namedtuple

import numpy as np
import pandas as pd


def calculate_realized_vola_stats(transactions: pd.DataFrame):

    # change the index to pandas Timestamps
    transactions.reset_index(inplace=True)
    # the date doesn't matter here
    transactions.set_index(pd.Timestamp("2019-01-01") +
                           pd.to_timedelta(transactions["timestamp"], unit="us"),
                           inplace=True)

    # high sampling frequency (short timescale)
    high_sampling_frequency = pd.Timedelta(5, "seconds")
    # keeping the last trade per high sampling frequency
    resampled_high_freq = transactions["mid"].resample(high_sampling_frequency,
                                                       label="right",
                                                       closed="right").last()
    # dealing with NaNs is important for return calculation below
    resampled_high_freq.dropna(inplace=True)
    num_obs = resampled_high_freq.shape[0]
    if num_obs == 0:
        return np.nan
    # taking log of the mid price (and scaling by 100)
    resampled_high_freq = np.log(resampled_high_freq) * 100
    log_returns = (resampled_high_freq - resampled_high_freq.shift(1))
    # Realized volatility for high frequency
    RV_all = np.sum(np.square(log_returns))
    noise_var_TSRV = RV_all / (2 * num_obs)

    # low sampling frequency (long timescale)
    low_sampling_frequency = pd.Timedelta(5, "minutes")
    frac = high_sampling_frequency / pd.Timedelta(1, "minute")
    offsets = [
        frac * multiple
        for multiple in range(int(low_sampling_frequency / high_sampling_frequency))
    ]
    all_realized_volas = list()
    all_ns = list()
    for offset in offsets:
        resampled_low_freq = resampled_high_freq.resample(low_sampling_frequency,
                                                          base=offset,
                                                          label="right",
                                                          closed="right").last()
        resampled_low_freq.dropna(inplace=True)
        resampled_low_freq = pd.DataFrame(resampled_low_freq)

        log_returns = (resampled_low_freq["mid"] - resampled_low_freq["mid"].shift(1))
        RV_offset = np.sum(np.square(log_returns))
        if offset == 0.0:
            RV_slow = RV_offset
        all_realized_volas.append(RV_offset)
        all_ns.append(log_returns.shape[0])

    # average of low freq estimates
    RV_average = np.mean(all_realized_volas)
    n_bar = np.mean(all_ns)

    # combine both for two-scales RV
    TSRV = RV_average - (n_bar / num_obs) * RV_all
    # small sample adjustment
    TSRV = 1 / (1 - n_bar / num_obs) * TSRV  #/ sum(delta))

    Delta = 1 / num_obs
    signal_to_noise = 2 * noise_var_TSRV / (TSRV * Delta + 2 * noise_var_TSRV)

    stats = dict(TSRV=TSRV,
                 noise_var_TSRV=noise_var_TSRV,
                 RV_slow=RV_slow,
                 signal_to_noise=signal_to_noise)
    return stats
