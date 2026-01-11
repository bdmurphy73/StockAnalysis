#!/usr/bin/env python3
"""Common technical indicators (vectorized, pandas-based).

Provides functions:
- sma(series, window)
- ema(series, span)
- macd(close, fast=12, slow=26, signal=9)
- rsi(close, period=14)
- bollinger_bands(close, window=20, num_std=2)
- momentum(close, n=10)
- stochastic_k_d(high, low, close, k_window=14, d_window=3)

Also provides `compute_indicators(df)` convenience that adds columns to a DataFrame
containing at least `Close`, `High`, and `Low` columns.
"""

from typing import Tuple
import pandas as pd
import numpy as np


def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    fast_ema = ema(close, fast)
    slow_ema = ema(close, slow)
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -1.0 * delta.clip(upper=0.0)
    # Wilder's smoothing
    ma_up = up.ewm(alpha=1.0/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1.0/period, adjust=False).mean()
    rs = ma_up / (ma_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0)


def bollinger_bands(close: pd.Series, window: int = 20, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    mid = sma(close, window)
    std = close.rolling(window=window, min_periods=1).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return mid, upper, lower


def momentum(close: pd.Series, n: int = 10) -> pd.Series:
    return close.diff(n)


def stochastic_k_d(high: pd.Series, low: pd.Series, close: pd.Series, k_window: int = 14, d_window: int = 3) -> Tuple[pd.Series, pd.Series]:
    lowest_low = low.rolling(window=k_window, min_periods=1).min()
    highest_high = high.rolling(window=k_window, min_periods=1).max()
    k = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
    d = k.rolling(window=d_window, min_periods=1).mean()
    return k.fillna(0), d.fillna(0)


def compute_indicators(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """Compute a set of common indicators and attach them to a copy of df.

    df must contain `Close`, `High`, `Low` columns. Returns a new DataFrame with
    indicator columns added. Default indicator parameters can be overridden via `config`.
    """
    if config is None:
        config = {}

    out = df.copy()
    close = out['Close']
    high = out['High']
    low = out['Low']

    # SMA / EMA
    out['SMA_20'] = sma(close, config.get('sma_20', 20))
    out['SMA_50'] = sma(close, config.get('sma_50', 50))
    out['EMA_12'] = ema(close, config.get('ema_12', 12))
    out['EMA_26'] = ema(close, config.get('ema_26', 26))

    # MACD
    macd_line, signal_line, hist = macd(close, config.get('macd_fast', 12), config.get('macd_slow', 26), config.get('macd_signal', 9))
    out['MACD'] = macd_line
    out['MACD_SIGNAL'] = signal_line
    out['MACD_HIST'] = hist

    # RSI
    out['RSI_14'] = rsi(close, config.get('rsi_14', 14))

    # Bollinger
    mid, upper, lower = bollinger_bands(close, config.get('bb_window', 20), config.get('bb_sigma', 2.0))
    out['BB_MID'] = mid
    out['BB_UP'] = upper
    out['BB_LOW'] = lower

    # Momentum
    out['MOM_10'] = momentum(close, config.get('mom_n', 10))

    # Stochastic
    k, d = stochastic_k_d(high, low, close, config.get('stoch_k', 14), config.get('stoch_d', 3))
    out['STOCH_K'] = k
    out['STOCH_D'] = d

    return out


if __name__ == '__main__':
    # Quick smoke test when run directly
    import pandas as pd
    dates = pd.date_range(end=pd.Timestamp.today(), periods=30)
    price = pd.Series(np.linspace(100, 120, len(dates)) + np.random.normal(0, 1, len(dates)), index=dates)
    df = pd.DataFrame({'Close': price, 'High': price * 1.01, 'Low': price * 0.99})
    out = compute_indicators(df)
    print(out.tail(3).to_string())
