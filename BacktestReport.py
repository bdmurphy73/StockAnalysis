#!/usr/bin/env python3
"""Replay backtest, produce per-trade log and summary metrics."""
import csv
import logging
from datetime import timedelta
import pandas as pd
from StockCommon import opendatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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


def replay_backtest(conn, start_date, end_date, starting_cash=1000.0):
    cal = get_trading_calendar(conn)
    days = [d for d in cal if start_date <= d <= end_date]
    cash = starting_cash
    trades = []
    for d in days:
        scores = get_scores_for_date(conn, d)
        if not scores:
            continue
        top = scores[0]['symbol']
        idx = days.index(d)
        if idx + 1 >= len(days):
            continue
        buy_date = days[idx + 1]
        sell_idx = idx + 6
        if sell_idx >= len(days):
            continue
        sell_date = days[sell_idx]
        buy_price = get_open_price(conn, top, buy_date)
        sell_price = get_open_price(conn, top, sell_date)
        if buy_price is None or sell_price is None:
            continue
        shares = int(cash // buy_price)
        if shares <= 0:
            continue
        cost = shares * buy_price
        cash -= cost
        proceeds = shares * sell_price
        profit = proceeds - cost
        cash += proceeds
        trades.append({
            'buy_date': buy_date,
            'sell_date': sell_date,
            'symbol': top,
            'buy_price': buy_price,
            'sell_price': sell_price,
            'shares': shares,
            'cost': cost,
            'proceeds': proceeds,
            'profit': profit,
            'pct_return': profit / cost if cost>0 else 0.0,
            'cash_after': cash
        })
    return trades, cash


def summarize_and_write(trades, start_cash, end_cash, out_csv='backtest_trades.csv'):
    # write CSV
    keys = ['buy_date','sell_date','symbol','buy_price','sell_price','shares','cost','proceeds','profit','pct_return','cash_after']
    with open(out_csv, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for t in trades:
            row = {k: t.get(k) for k in keys}
            w.writerow(row)
    # metrics
    n = len(trades)
    wins = sum(1 for t in trades if t['profit']>0)
    total_profit = sum(t['profit'] for t in trades)
    avg_pct = (sum(t['pct_return'] for t in trades)/n) if n>0 else 0.0
    summary = {
        'trades': n,
        'wins': wins,
        'win_rate': wins / n if n>0 else 0.0,
        'starting_cash': start_cash,
        'ending_cash': end_cash,
        'total_profit': total_profit,
        'avg_pct_return_per_trade': avg_pct
    }
    return summary


if __name__ == '__main__':
    import argparse
    from datetime import timedelta
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', help='Start date YYYY-MM-DD', required=False)
    parser.add_argument('--end', help='End date YYYY-MM-DD', required=False)
    args = parser.parse_args()
    conn = opendatabase()
    cal = get_trading_calendar(conn)
    end = pd.to_datetime(args.end).date() if args.end else cal[-1]
    start = pd.to_datetime(args.start).date() if args.start else (end - timedelta(days=365))
    trades, end_cash = replay_backtest(conn, start, end, starting_cash=1000.0)
    summary = summarize_and_write(trades, 1000.0, end_cash)
    print('Backtest summary:')
    for k,v in summary.items():
        print(f'{k}: {v}')
    print('Wrote trade log to backtest_trades.csv')
    conn.close()
