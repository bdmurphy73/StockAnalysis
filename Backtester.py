#!/usr/bin/env python3
"""
Backtester that uses `calcuresults` scores to simulate buying top stock each day
and selling 5 trading days later. Stores results in DB and emails summary.
"""
import os
import logging
import smtplib
from email.message import EmailMessage
from datetime import timedelta

import pandas as pd

from StockCommon import opendatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def ensure_results_table(conn):
    q = '''CREATE TABLE IF NOT EXISTS stock_backtest_results (
        id INT AUTO_INCREMENT PRIMARY KEY,
        start_date DATE,
        end_date DATE,
        starting_cash DOUBLE,
        ending_cash DOUBLE,
        notes TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'''
    with conn.cursor() as cur:
        cur.execute(q)
    conn.commit()


def get_trading_calendar(conn):
    # Use cursor fetch to avoid pandas DBAPI quirks and filter out malformed rows
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT hdate FROM stockhistory ORDER BY hdate")
        rows = cur.fetchall()
    dates = []
    for r in rows:
        # r may be {'hdate': date} or tuple depending on cursor; handle both
        if isinstance(r, dict):
            val = r.get('hdate')
        elif isinstance(r, (list, tuple)):
            val = r[0] if r else None
        else:
            val = None
        try:
            if val is None:
                continue
            # convert to date
            d = pd.to_datetime(val).date()
            dates.append(d)
        except Exception:
            continue
    return sorted(list(dict.fromkeys(dates)))


def get_calc_scores_for_date(conn, date):
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


def run_backtest(conn, start_date, end_date, starting_cash=1000.0):
    ensure_results_table(conn)
    calendar = get_trading_calendar(conn)
    # Normalize inputs to date objects
    from datetime import date as _date
    def _to_date(d):
        if d is None:
            return None
        if isinstance(d, _date):
            return d
        try:
            return pd.to_datetime(d).date()
        except Exception:
            return None

    start_date = _to_date(start_date)
    end_date = _to_date(end_date)

    if start_date not in calendar:
        # find next available date
        calendar_sorted = sorted(calendar)
        start_date = next((d for d in calendar_sorted if d >= start_date), None)
    if end_date not in calendar:
        calendar_sorted = sorted(calendar)
        end_date = next((d for d in reversed(calendar_sorted) if d <= end_date), None)
    if start_date is None or end_date is None:
        logging.error("Invalid start or end date for backtest")
        return None

    # consider trading days between start and end
    days = [d for d in calendar if start_date <= d <= end_date]
    cash = starting_cash
    for d in days:
        scores = get_calc_scores_for_date(conn, d)
        if not scores:
            continue
        # scores may be dicts or tuples; normalize to list of (symbol, rslt)
        norm = []
        for s in scores:
            if isinstance(s, dict):
                norm.append((s.get('symbol'), s.get('rslt')))
            else:
                norm.append((s[0], s[1] if len(s) > 1 else None))
        # apply minimum score filter if set on conn (injected via attribute)
        min_score = getattr(conn, '_min_score', 0.0)
        filtered = [t for t in norm if t[1] is not None and t[1] >= min_score]
        if not filtered:
            continue
        top_symbol = filtered[0][0]
        # find buy date (next trading date after d)
        idx = days.index(d)
        if idx + 1 >= len(days):
            continue
        buy_date = days[idx + 1]
        sell_idx = idx + 6  # buy + 5 trading days later means index + 6 from original d
        if sell_idx >= len(days):
            continue
        sell_date = days[sell_idx]
        buy_price = get_open_price(conn, top_symbol, buy_date)
        sell_price = get_open_price(conn, top_symbol, sell_date)
        if buy_price is None or sell_price is None:
            continue
        shares = int(cash // buy_price)
        if shares <= 0:
            continue
        cost = shares * buy_price
        cash -= cost
        proceeds = shares * sell_price
        cash += proceeds
    # store results - convert dates to ISO strings to avoid NaT insertion
    start_iso = start_date.isoformat() if hasattr(start_date, 'isoformat') else None
    end_iso = end_date.isoformat() if hasattr(end_date, 'isoformat') else None
    with conn.cursor() as cur:
        cur.execute("INSERT INTO stock_backtest_results (start_date, end_date, starting_cash, ending_cash, notes) VALUES (%s, %s, %s, %s, %s)", (start_iso, end_iso, starting_cash, cash, 'Automated backtest'))
    conn.commit()
    return cash


def send_email(subject, body, to_addr):
    host = os.environ.get('SMTP_HOST')
    port = int(os.environ.get('SMTP_PORT', '587'))
    user = os.environ.get('SMTP_USER')
    pwd = os.environ.get('SMTP_PASS')
    use_tls = os.environ.get('SMTP_TLS', 'true').lower() in ('1', 'true', 'yes')
    use_ssl = os.environ.get('SMTP_SSL', 'false').lower() in ('1', 'true', 'yes')
    from_addr = os.environ.get('SMTP_FROM') or user
    if to_addr is None:
        to_addr = os.environ.get('SMTP_TO')
    if not host or not user or not pwd or not to_addr:
        logging.warning('SMTP configuration or recipient not found in environment; skipping email')
        return False
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg.set_content(body)
    try:
        if use_ssl or port == 465:
            with smtplib.SMTP_SSL(host, port) as s:
                s.login(user, pwd)
                s.send_message(msg)
        else:
            with smtplib.SMTP(host, port) as s:
                if use_tls:
                    s.starttls()
                s.login(user, pwd)
                s.send_message(msg)
        logging.info('Email sent to %s', to_addr)
        return True
    except Exception as e:
        logging.error('Failed to send email: %s', e)
        return False


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', help='Start date YYYY-MM-DD', required=False)
    parser.add_argument('--end', help='End date YYYY-MM-DD', required=False)
    parser.add_argument('--min-score', type=float, default=0.0, help='Minimum score threshold to trade')
    args = parser.parse_args()
    conn = opendatabase()
    # attach min-score to connection for use in run_backtest
    conn._min_score = float(args.min_score) if args.min_score is not None else 0.0
    if conn is None:
        raise SystemExit('Cannot open DB')
    cal = get_trading_calendar(conn)
    if not cal:
        raise SystemExit('No trading calendar')
    from datetime import timedelta
    end = pd.to_datetime(args.end).date() if args.end else cal[-1]
    if args.start:
        start = pd.to_datetime(args.start).date()
    else:
        start = end - timedelta(days=365)
    ending_cash = run_backtest(conn, start, end, starting_cash=1000.0)
    if ending_cash is not None:
        body = f'Backtest completed from {start} to {end}. Ending cash: ${ending_cash:.2f}'
        # send to SMTP_TO (or SMTP_TO env var) when configured
        send_email('Backtest results', body, None)
    conn.close()
