#!/usr/bin/env python3
"""Score engine to compute daily technical indicator scores and rank symbols.

Usage: import functions or run as script to evaluate top symbols.

Functions:
- score_from_df(df, config) -> dict
- evaluate_symbol(dbconn, symbol, lookback_days, config) -> dict
- rank_symbols(dbconn, symbols=None, lookback_days=60, threshold=90, config=None, write_table=False) -> list

Optional: when `write_table=True` results are inserted into a `daily_scores` table.
"""

from datetime import date
import json
import logging
from typing import List, Dict, Any, Optional

from StockCommon import opendatabase, closedatabase
from StockPgresDB import stock_db_tables
import indicators as ind
import pymysql
import time
import concurrent.futures

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    'weights': {
        'macd': 1.5,
        'sma20': 1.0,
        'sma50': 1.0,
        'ema': 1.0,
        'rsi': 1.0,
        'bb': 0.5,
        'stoch': 1.0,
    },
    # parameters passed to indicators.compute_indicators
    'ind_params': {},
}


def score_from_df(df, config: dict = None) -> Dict[str, Any]:
    """Compute a score (0-100) for the latest row in `df`.

    `df` should contain columns Close, High, Low and enough history for indicators.
    Returns a dict with `score` (percentage) and `signals` details.
    """
    if config is None:
        config = DEFAULT_CONFIG
    weights = config.get('weights', DEFAULT_CONFIG['weights'])

    data = ind.compute_indicators(df, config.get('ind_params'))
    if data.empty:
        return {'score': 0.0, 'signals': {}, 'raw': 0.0}

    last = data.iloc[-1]
    prev = data.iloc[-2] if len(data) > 1 else last

    wsum = 0.0
    max_positive = sum(v for v in weights.values() if v > 0)
    signals = {}

    # MACD histogram positive
    macd_hist = float(last.get('MACD_HIST', 0) or 0)
    signals['macd_hist_pos'] = macd_hist > 0
    if macd_hist > 0:
        wsum += weights.get('macd', 0)

    close = float(last['Close'])

    # SMA/EMA conditions
    signals['close_above_sma20'] = close > float(last.get('SMA_20', close))
    if signals['close_above_sma20']:
        wsum += weights.get('sma20', 0)

    signals['close_above_sma50'] = close > float(last.get('SMA_50', close))
    if signals['close_above_sma50']:
        wsum += weights.get('sma50', 0)

    signals['ema12_above_ema26'] = float(last.get('EMA_12', 0)) > float(last.get('EMA_26', 0))
    if signals['ema12_above_ema26']:
        wsum += weights.get('ema', 0)

    # RSI
    r = float(last.get('RSI_14', 50))
    signals['rsi'] = r
    if r < 30:
        wsum += weights.get('rsi', 0)
    elif r > 70:
        wsum -= weights.get('rsi', 0)

    # Bollinger: bonus if price between lower and mid band (potential bounce) or above mid
    bb_low = float(last.get('BB_LOW', close))
    bb_mid = float(last.get('BB_MID', close))
    signals['bb_position'] = 'above_mid' if close > bb_mid else ('between' if close >= bb_low else 'below_low')
    if close > bb_mid:
        wsum += weights.get('bb', 0) * 0.5
    elif bb_low <= close <= bb_mid:
        wsum += weights.get('bb', 0)

    # Stochastic bullish cross
    k = float(last.get('STOCH_K', 0))
    d = float(last.get('STOCH_D', 0))
    pk = float(prev.get('STOCH_K', k))
    pdv = float(prev.get('STOCH_D', d))
    stoch_cross = (k > d) and (pk <= pdv)
    signals['stoch_bull_cross'] = stoch_cross
    if stoch_cross:
        wsum += weights.get('stoch', 0)

    # Normalize
    score_pct = max(0.0, min(100.0, 100.0 * (wsum / max_positive) if max_positive else 0.0))

    return {'score': round(score_pct, 2), 'signals': signals, 'raw': round(wsum, 3)}


def _create_scores_table(conn):
    sql = '''
    CREATE TABLE IF NOT EXISTS daily_scores (
        id INT AUTO_INCREMENT PRIMARY KEY,
        score_date DATE NOT NULL,
        symbol VARCHAR(32) NOT NULL,
        score DOUBLE NOT NULL,
        indicators JSON,
        suggestion JSON,
        UNIQUE KEY uniq_date_symbol (score_date, symbol)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()



def evaluate_symbol(conn, symbol: str, lookback_days: int = 60, config: dict = None) -> Dict[str, Any]:
    """Load history for `symbol` from DB and compute score."""
    # read last `lookback_days` records from stockhistory
    q = f"SELECT HDATE, OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME FROM `{stock_db_tables['His']}` WHERE SYMBOL=%s ORDER BY HDATE DESC LIMIT %s"
    rows = []
    try:
        with conn.cursor() as cur:
            cur.execute(q, (symbol, lookback_days))
            rows = cur.fetchall()
    except pymysql.Error as e:
        logging.error("DB error fetching history for %s: %s", symbol, e)
        return {'symbol': symbol, 'score': 0.0, 'error': str(e)}

    if not rows:
        return {'symbol': symbol, 'score': 0.0, 'error': 'no_history'}

    # rows are newest-first; convert to DataFrame oldest-first
    import pandas as pd
    df = pd.DataFrame(rows)
    df = df.rename(columns={'HDATE': 'Date', 'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low', 'CLOSE': 'Close', 'ADJCLOSE': 'AdjClose', 'VOLUME': 'Volume'})
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')
    df = df.reset_index(drop=True)

    res = score_from_df(df, config)
    res_out = {'symbol': symbol, 'score': res['score'], 'signals': res['signals'], 'raw': res['raw']}
    return res_out


def rank_symbols(conn=None, symbols: Optional[List[str]] = None, lookback_days: int = 60, threshold: float = 90.0, config: dict = None, write_table: bool = False, workers: int = 8, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
    """Compute scores for `symbols` (or all active symbols if None) and return those >= threshold sorted desc."""
    close_conn = False
    if conn is None:
        conn = opendatabase()
        close_conn = True
    if conn is None:
        raise RuntimeError("Cannot open DB connection")

    # optionally create table
    if write_table:
        _create_scores_table(conn)

    if symbols is None:
        # fetch from stockinfo
        from StockPgresDB import getstocklist
        symbols = [s.get('SYMBOL') if isinstance(s, dict) else s[0] for s in getstocklist(conn)]

    results = []
    start = time.time()
    logger.info('Scoring %d symbols with %d workers', len(symbols), workers)
    # use ThreadPoolExecutor to parallelize DB reads + indicator calculations
    def _score(sym):
        c = None
        try:
            c = opendatabase()
            return evaluate_symbol(c, sym, lookback_days, config)
        except Exception as e:
            logger.exception('Error scoring %s', sym)
            return {'symbol': sym, 'score': 0.0, 'error': str(e)}
        finally:
            if c:
                try:
                    closedatabase(c)
                except Exception:
                    logger.debug('Error closing worker DB connection for %s', sym)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_score, s): s for s in symbols}
        for fut in concurrent.futures.as_completed(futs, timeout=timeout):
            r = fut.result()
            if r:
                results.append(r)

    # perform DB writes in single thread to avoid connection sharing issues
    if write_table:
        for r in results:
            if 'score' in r:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO daily_scores (score_date, symbol, score, indicators)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                score=VALUES(score),
                                indicators=VALUES(indicators)
                            """,
                            (date.today(), r.get('symbol'), r.get('score', 0.0), json.dumps(r.get('signals', {})))
                        )
                    conn.commit()
                except Exception:
                    logger.exception('Failed to write score for %s', r.get('symbol'))

    logger.info('Scoring completed in %.2fs', time.time() - start)

    # Filter and sort
    passed = [r for r in results if r.get('score', 0) >= threshold]
    passed = sorted(passed, key=lambda x: x.get('score', 0), reverse=True)

    if close_conn:
        closedatabase(conn)

    return passed


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description='Run score engine')
    p.add_argument('--write', action='store_true', help='Write scores into daily_scores table')
    p.add_argument('--lookback', type=int, default=90, help='History lookback days')
    p.add_argument('--threshold', type=float, default=0.0, help='Threshold to filter results')
    p.add_argument('--top', type=int, default=10, help='Show top N results')
    p.add_argument('--symbols', type=str, help='Optional comma-separated list of symbols to score')
    p.add_argument('--workers', type=int, default=8, help='Number of parallel worker threads')
    p.add_argument('--timeout', type=float, default=None, help='Optional seconds timeout for scoring')
    args = p.parse_args()

    conn = opendatabase()
    if conn is None:
        print('Cannot open DB')
        raise SystemExit(1)

    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]

    tops = rank_symbols(conn, symbols=symbols, lookback_days=args.lookback, threshold=args.threshold, config=None, write_table=args.write, workers=args.workers, timeout=args.timeout)
    # print top results
    for r in tops[:args.top]:
        print(f"{r['symbol']}: {r['score']} — {r.get('signals')}")

    closedatabase(conn)
