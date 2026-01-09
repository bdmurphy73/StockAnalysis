#!/usr/bin/env python3
"""
Optimizer for backtest parameters.

This script performs randomized search over simple strategy parameters
and evaluates each configuration by replaying the backtest using
`calcuresults` and `stockhistory`. Results are saved to an
`optimizer_results` table in the database.

Usage:
  python Optimizer.py --trials 50

Parameters searched:
  - top_k: number of top scored stocks to consider (1..5)
  - hold_days: holding period in trading days (3..10)
  - position_fraction: fraction of cash to allocate per trade (0.2..1.0)
  - min_score_pct: minimum score percentile to consider (0.0..0.5)

The objective chosen is ending cash (primary) and win rate (secondary).
"""
import json
import logging
import random
from datetime import timedelta, datetime, timezone
import pandas as pd

from StockCommon import opendatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def ensure_optimizer_table(conn):
    q = '''CREATE TABLE IF NOT EXISTS optimizer_results (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ts DATETIME,
        params JSON,
        start_date DATE,
        end_date DATE,
        starting_cash DOUBLE,
        ending_cash DOUBLE,
        trades INT,
        wins INT,
        win_rate DOUBLE,
        notes TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'''
    with conn.cursor() as cur:
        cur.execute(q)
    conn.commit()


def get_trading_calendar(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT hdate FROM stockhistory ORDER BY hdate")
        rows = cur.fetchall()
    dates = []
    for r in rows:
        if isinstance(r, dict):
            val = r.get('hdate')
        elif isinstance(r, (list, tuple)):
            val = r[0] if r else None
        else:
            val = None
        try:
            if val is None:
                continue
            d = pd.to_datetime(val).date()
            dates.append(d)
        except Exception:
            continue
    return sorted(list(dict.fromkeys(dates)))


def get_scores_for_date(conn, date):
    q = "SELECT symbol, rslt FROM calcuresults WHERE ldate = %s ORDER BY rslt DESC"
    with conn.cursor() as cur:
        cur.execute(q, (date,))
        rows = cur.fetchall()
    return rows


def get_open_price(conn, symbol, date):
    q = "SELECT open FROM stockhistory WHERE symbol=%s AND hdate=%s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(q, (symbol, date))
        r = cur.fetchone()
    return r['open'] if r else None


def simulate_strategy(conn, start_date, end_date, starting_cash, top_k=1, hold_days=5, position_fraction=1.0, min_score_pct=0.0):
    cal = get_trading_calendar(conn)
    days = [d for d in cal if start_date <= d <= end_date]
    if not days:
        return {'ending_cash': starting_cash, 'trades': 0, 'wins': 0}
    cash = starting_cash
    trades = 0
    wins = 0
    # precompute score percentiles per date
    for i, d in enumerate(days):
        scores = get_scores_for_date(conn, d)
        if not scores:
            continue
        # filter by min_score_pct (percentile of available scores)
        vals = [r['rslt'] for r in scores]
        thresh = None
        if vals:
            thresh = pd.Series(vals).quantile(min_score_pct)
        candidates = [r for r in scores if thresh is None or r['rslt'] >= thresh]
        if not candidates:
            continue
        picks = candidates[:top_k]
        # buy on next trading day
        if i + 1 >= len(days):
            continue
        buy_date = days[i + 1]
        sell_idx = i + 1 + hold_days
        if sell_idx >= len(days):
            continue
        sell_date = days[sell_idx]
        # allocate cash equally across picks up to position_fraction of cash
        allocatable = cash * position_fraction
        per_pick = allocatable / len(picks)
        for p in picks:
            symbol = p['symbol']
            buy_price = get_open_price(conn, symbol, buy_date)
            sell_price = get_open_price(conn, symbol, sell_date)
            if buy_price is None or sell_price is None:
                continue
            shares = int(per_pick // buy_price)
            if shares <= 0:
                continue
            cost = shares * buy_price
            cash -= cost
            proceeds = shares * sell_price
            profit = proceeds - cost
            cash += proceeds
            trades += 1
            if profit > 0:
                wins += 1
    return {'ending_cash': cash, 'trades': trades, 'wins': wins}


def random_search(conn, start_date, end_date, starting_cash=1000.0, trials=50, seed=42):
    random.seed(seed)
    best = None
    for t in range(trials):
        params = {
            'top_k': random.randint(1, 5),
            'hold_days': random.randint(3, 10),
            'position_fraction': round(random.uniform(0.2, 1.0), 2),
            'min_score_pct': round(random.uniform(0.0, 0.5), 2)
        }
        res = simulate_strategy(conn, start_date, end_date, starting_cash, **params)
        trades = res['trades']
        wins = res['wins']
        win_rate = (wins / trades) if trades > 0 else 0.0
        ending = res['ending_cash']
        logging.info('Trial %d params=%s -> ending=%.2f trades=%d win_rate=%.2f', t+1, params, ending, trades, win_rate)
        score = (ending, win_rate)
        # persist this trial's result
        try:
            save_result(conn, params, start_date, end_date, starting_cash, ending, trades, wins, win_rate, notes=f'trial {t+1}')
        except Exception:
            logging.exception('Failed to save trial %d to DB', t+1)
        # keep best by ending cash, tie-breaker win_rate
        if best is None or ending > best['ending_cash'] or (ending == best['ending_cash'] and win_rate > best['win_rate']):
            best = {'params': params, 'ending_cash': ending, 'trades': trades, 'wins': wins, 'win_rate': win_rate}
    return best


def save_result(conn, params, start_date, end_date, starting_cash, ending_cash, trades, wins, win_rate, notes=None):
    ensure_optimizer_table(conn)
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    notes_val = notes if notes is not None else 'random_search'
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO optimizer_results (ts, params, start_date, end_date, starting_cash, ending_cash, trades, wins, win_rate, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (ts, json.dumps(params), start_date, end_date, starting_cash, ending_cash, trades, wins, win_rate, notes_val)
        )
    conn.commit()


if __name__ == '__main__':
    import argparse
    from datetime import timedelta
    parser = argparse.ArgumentParser()
    parser.add_argument('--trials', type=int, default=50)
    parser.add_argument('--start', help='Start date YYYY-MM-DD', required=False)
    parser.add_argument('--end', help='End date YYYY-MM-DD', required=False)
    args = parser.parse_args()
    conn = opendatabase()
    cal = get_trading_calendar(conn)
    if not cal:
        raise SystemExit('No calendar')
    end = pd.to_datetime(args.end).date() if args.end else cal[-1]
    start = pd.to_datetime(args.start).date() if args.start else (end - timedelta(days=365))
    best = random_search(conn, start, end, starting_cash=1000.0, trials=args.trials)
    if best:
        logging.info('Best found: %s', best)
        save_result(conn, best['params'], start, end, 1000.0, best['ending_cash'], best['trades'], best['wins'], best['win_rate'], notes='best')
    conn.close()
