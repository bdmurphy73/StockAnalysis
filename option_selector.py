#!/usr/bin/env python3
"""Select call option suggestions for top scored symbols and persist to DB.

The script queries `daily_scores` for the latest score_date (or a provided date),
selects top symbols by score or threshold, fetches options chains via yfinance,
computes approximate Black-Scholes delta for calls, and chooses a strike nearest
to the target delta (default 0.30). Results are saved back to `daily_scores.suggestion`.
"""
from datetime import date, datetime, timedelta
import json
import logging
import math
from typing import Optional, List

import yfinance as yf
import pymysql

from StockCommon import opendatabase, closedatabase

DEFAULT_RF = 0.02


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_delta_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return norm_cdf(d1)


def choose_option_for_symbol(sym: str, target_delta: float = 0.3, min_days: int = 7, max_days: int = 14, rf: float = DEFAULT_RF, min_mid: float = 2.0, min_bid: float = 0.5, delta_tol: float = 0.05, min_volume: int = 20, min_oi: int = 20, timeout: Optional[float] = None):
    t = yf.Ticker(sym)
    try:
        spot = float(t.history(period='5d')['Close'].dropna().iloc[-1])
    except Exception:
        return None

    # find expirations in desired window
    exps = []
    try:
        for e in t.options:
            try:
                ed = datetime.strptime(e, '%Y-%m-%d').date()
            except Exception:
                continue
            days = (ed - date.today()).days
            if days >= min_days and days <= max_days:
                exps.append((ed, days))
    except Exception:
        return None

    if not exps:
        # fallback: pick nearest expiration >= min_days up to 30
        try:
            for e in t.options:
                ed = datetime.strptime(e, '%Y-%m-%d').date()
                days = (ed - date.today()).days
                if days >= min_days and days <= 30:
                    exps.append((ed, days))
        except Exception:
            return None

    if not exps:
        return None

    # prefer the nearest expiration within window
    exps.sort(key=lambda x: x[1])

    best_choice = None
    best_delta_diff = 999.0

    for ed, days in exps:
        try:
            chain = t.option_chain(ed.strftime('%Y-%m-%d'))
            calls = chain.calls
        except Exception:
            continue

        # estimate historical vol if implied vol missing
        hist_sigma = None
        try:
            hist = t.history(period='120d')['Close'].dropna()
            if len(hist) > 10:
                ret = hist.pct_change().dropna()
                hist_sigma = float(ret.std() * math.sqrt(252))
        except Exception:
            hist_sigma = None

        for _, row in calls.iterrows():
            strike = float(row.get('strike') or row.get('Strike') or 0)
            # use raw values where possible and coerce to floats/ints safely
            raw_bid = row.get('bid') if 'bid' in row.index else row.get('Bid') if 'Bid' in row.index else None
            raw_ask = row.get('ask') if 'ask' in row.index else row.get('Ask') if 'Ask' in row.index else None
            try:
                bid = float(raw_bid) if raw_bid is not None else float('nan')
            except Exception:
                bid = float('nan')
            try:
                ask = float(raw_ask) if raw_ask is not None else float('nan')
            except Exception:
                ask = float('nan')
            # determine mid price
            if math.isfinite(bid) and math.isfinite(ask):
                mid = (bid + ask) / 2.0
            else:
                try:
                    mid = float(row.get('lastPrice') or row.get('last') or float('nan'))
                except Exception:
                    mid = float('nan')

            # explicit liquidity filters: require finite values and thresholds
            if not math.isfinite(mid) or mid < min_mid:
                continue
            if not math.isfinite(bid) or bid < min_bid:
                continue
            # check option volume/openInterest if available
            try:
                vol = int(row.get('volume') or 0)
            except Exception:
                vol = 0
            try:
                oi = int(row.get('openInterest') or row.get('OpenInterest') or 0)
            except Exception:
                oi = 0
            if vol < min_volume or oi < min_oi:
                continue
            imp = row.get('impliedVolatility') if 'impliedVolatility' in row.index else row.get('impliedVol') if 'impliedVol' in row.index else None
            try:
                sigma = float(imp) if imp is not None and not math.isnan(float(imp)) else (hist_sigma or 0.2)
            except Exception:
                sigma = hist_sigma or 0.2

            T = max(1, days) / 365.0
            delta = bs_delta_call(spot, strike, T, rf, sigma)
            delta_diff = abs(delta - target_delta)

            if delta_diff < best_delta_diff:
                best_delta_diff = delta_diff
                best_choice = {
                    'symbol': sym,
                    'expiration': ed.isoformat(),
                    'days_to_exp': days,
                    'strike': strike,
                    'bid': bid,
                    'ask': ask,
                    'mid': mid,
                    'impliedVol': sigma,
                    'delta': round(delta, 4),
                }

            # if within acceptable band, pick it immediately
            if delta >= (target_delta - delta_tol) and delta <= (target_delta + delta_tol):
                return best_choice

    return best_choice


def persist_suggestion(conn: pymysql.connections.Connection, score_date: date, symbol: str, suggestion: dict):
    if suggestion is None:
        return
    # sanitize suggestion to valid JSON (replace NaN/Inf with null)
    def _sanitize(o):
        if o is None:
            return None
        if isinstance(o, dict):
            return {k: _sanitize(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_sanitize(v) for v in o]
        try:
            # convert numpy types and plain numbers
            if isinstance(o, (int, float, str, bool)):
                if isinstance(o, float):
                    if o != o or o == float('inf') or o == float('-inf'):
                        return None
                return o
        except Exception:
            pass
        # fallback to string
        return str(o)

    safe_suggestion = _sanitize(suggestion)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_scores (score_date, symbol, score, indicators, suggestion)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE suggestion=VALUES(suggestion)
            """,
            (score_date, symbol, 0.0, json.dumps({}), json.dumps(safe_suggestion))
        )
    conn.commit()


def main(top_n: int = 10, threshold: float = 90.0, date_str: Optional[str] = None, target_delta: float = 0.3, min_days: int = 7, max_days: int = 14, min_mid: float = 2.0, min_bid: float = 0.5, delta_tol: float = 0.05, min_volume: int = 20, min_oi: int = 20, workers: int = 6):
    conn = opendatabase()
    if conn is None:
        logging.error('Cannot open DB')
        return

    # determine score_date
    score_date = None
    if date_str:
        score_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    else:
        with conn.cursor() as cur:
            cur.execute('SELECT MAX(score_date) as sd FROM daily_scores')
            r = cur.fetchone()
            if r:
                score_date = r.get('sd') if isinstance(r, dict) else r[0]
    if score_date is None:
        logging.error('No score_date found in daily_scores')
        closedatabase(conn)
        return

    # fetch top symbols
    with conn.cursor() as cur:
        if threshold > 0:
            cur.execute('SELECT symbol, score FROM daily_scores WHERE score_date=%s AND score>=%s ORDER BY score DESC LIMIT %s', (score_date, threshold, top_n))
        else:
            cur.execute('SELECT symbol, score FROM daily_scores WHERE score_date=%s ORDER BY score DESC LIMIT %s', (score_date, top_n))
        rows = cur.fetchall()

    symbols = [r.get('symbol') if isinstance(r, dict) else r[0] for r in rows]

    logging.info('Selecting options for %d symbols (date=%s)', len(symbols), score_date)

    # parallelize option selection
    import concurrent.futures
    def _choose_and_persist(sym):
        try:
            c = choose_option_for_symbol(sym, target_delta=target_delta, min_days=min_days, max_days=max_days, min_mid=min_mid, min_bid=min_bid, delta_tol=delta_tol, min_volume=min_volume, min_oi=min_oi)
            if c:
                # only persist suggestions that pass basic liquidity checks (double-check here)
                try:
                    mid_f = float(c.get('mid'))
                except Exception:
                    mid_f = float('nan')
                try:
                    bid_f = float(c.get('bid'))
                except Exception:
                    bid_f = float('nan')
                if math.isfinite(mid_f) and math.isfinite(bid_f):
                    persist_suggestion(conn, score_date, sym, c)
                    logging.info('Saved suggestion for %s: %s', sym, c)
                else:
                    logging.info('Suggestion for %s failed final liquidity check: %s', sym, c)
            else:
                logging.info('No suitable option found for %s', sym)
        except Exception:
            logging.exception('Error selecting option for %s', sym)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_choose_and_persist, symbols))

    closedatabase(conn)


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Option selector for top scored symbols')
    parser.add_argument('--top', type=int, default=10)
    parser.add_argument('--threshold', type=float, default=90.0)
    parser.add_argument('--date', type=str, help='score_date YYYY-MM-DD (optional)')
    parser.add_argument('--target_delta', type=float, default=0.3, help='Target call delta')
    parser.add_argument('--min_days', type=int, default=7, help='Min days to expiration')
    parser.add_argument('--max_days', type=int, default=14, help='Max days to expiration')
    parser.add_argument('--min_mid', type=float, default=2.0, help='Minimum mid price for option')
    parser.add_argument('--min_bid', type=float, default=0.5, help='Minimum bid for option')
    parser.add_argument('--delta_tol', type=float, default=0.05, help='Delta tolerance around target')
    parser.add_argument('--min_volume', type=int, default=20, help='Minimum option contract volume')
    parser.add_argument('--min_oi', type=int, default=20, help='Minimum option open interest')
    parser.add_argument('--workers', type=int, default=6, help='Parallel workers for option selection')
    args = parser.parse_args()
    main(top_n=args.top, threshold=args.threshold, date_str=args.date, target_delta=args.target_delta, min_days=args.min_days, max_days=args.max_days, min_mid=args.min_mid, min_bid=args.min_bid, delta_tol=args.delta_tol, min_volume=args.min_volume, min_oi=args.min_oi, workers=args.workers)
