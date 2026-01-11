#!/usr/bin/env python3
"""Build a pipeline summary from `daily_scores` and send email.

Usage: run with --send to actually send via SMTP (env vars required), otherwise prints
the message (dry-run).
"""
import os
import json
import logging
from datetime import date
from email.message import EmailMessage
import smtplib

from StockCommon import opendatabase, closedatabase

SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USER = os.environ.get('SMTP_USER')
SMTP_PASS = os.environ.get('SMTP_PASS')
SMTP_FROM = os.environ.get('SMTP_FROM', SMTP_USER)
SMTP_TO = os.environ.get('SMTP_TO') or os.environ.get('NOTIFY_TO')

logger = logging.getLogger(__name__)


def build_report(limit=20):
    conn = opendatabase()
    cur = conn.cursor()
    cur.execute('SELECT symbol, score, indicators, suggestion FROM daily_scores WHERE score_date=CURDATE() ORDER BY score DESC LIMIT %s', (limit,))
    rows = cur.fetchall()
    closedatabase(conn)
    out = []
    for r in rows:
        if isinstance(r, dict):
            sym = r.get('symbol')
            score = r.get('score')
            indicators = json.loads(r.get('indicators') or '{}')
            suggestion = json.loads(r.get('suggestion') or 'null') if r.get('suggestion') else None
        else:
            sym, score, indicators, suggestion = r[0], r[1], json.loads(r[2] or '{}'), json.loads(r[3]) if r[3] else None
        out.append({'symbol': sym, 'score': score, 'indicators': indicators, 'suggestion': suggestion})
    return out


def render_plain(report):
    if not report:
        return 'No scored symbols today.'
    lines = []
    for i, r in enumerate(report, start=1):
        sline = f"{i}. {r['symbol']} — score={r['score']}"
        if r['suggestion']:
            sug = r['suggestion']
            sline += f" | suggestion: {sug.get('expiration')} strike={sug.get('strike')} delta={sug.get('delta')} mid={sug.get('mid')}"
        lines.append(sline)
    return "\n".join(lines)


def send(subject, body):
    if not SMTP_USER or not SMTP_PASS or not SMTP_TO:
        raise RuntimeError('SMTP_USER, SMTP_PASS, SMTP_TO must be set to send email')
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = SMTP_FROM
    msg['To'] = SMTP_TO
    msg.set_content(body)
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)


def main(send_email_flag: bool = False, limit: int = 20):
    report = build_report(limit=limit)
    body = render_plain(report)
    subject = f"Daily pipeline report — {date.today().isoformat()}"
    if send_email_flag:
        send(subject, body)
        print('Email sent; items:', len(report))
    else:
        print('DRY RUN:')
        print(subject)
        print(body)


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument('--send', action='store_true', help='Actually send email via SMTP')
    p.add_argument('--limit', type=int, default=20)
    args = p.parse_args()
    main(send_email_flag=args.send, limit=args.limit)
