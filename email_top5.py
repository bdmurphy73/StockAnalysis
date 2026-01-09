#!/usr/bin/env python3
"""Compute today's top 5 symbols by score and email results.

Uses the existing DB helper `StockCommon.opendatabase()` and SMTP env vars.
If no symbol meets the minimum cutoff the email body will say "No stocks today.".
"""
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
import json

import StockCommon

# Minimum cutoff discussed earlier
MIN_SCORE = float(os.environ.get("MIN_SCORE", "0.545798"))

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER)
SMTP_TO = os.environ.get("SMTP_TO") or os.environ.get("NOTIFY_TO")

if not SMTP_USER or not SMTP_PASS or not SMTP_TO:
    raise SystemExit("SMTP_USER, SMTP_PASS and SMTP_TO must be set in the environment")

def get_top_scores(limit=5, min_score=MIN_SCORE):
    db = StockCommon.opendatabase()
    cur = db.cursor()
    # calcuresults table uses `ldate` for the date and `rslt` for the score
    sql = (
        "SELECT symbol, rslt FROM calcuresults "
        "WHERE ldate = (SELECT MAX(ldate) FROM calcuresults) "
        "AND rslt >= %s ORDER BY rslt DESC LIMIT %s"
    )
    try:
        cur.execute(sql, (min_score, limit))
        rows = cur.fetchall()
        out = []
        for r in rows:
            if isinstance(r, (list, tuple)):
                sym = r[0]
                sc = r[1]
            else:
                # cursor may return dict-like rows
                sym = r.get("symbol") or r.get("sym")
                sc = r.get("rslt") or r.get("score")
            try:
                out.append((sym, float(sc)))
            except Exception:
                continue
        return out
    finally:
        try:
            cur.close()
            db.close()
        except Exception:
            pass

def send_email(subject, body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = SMTP_TO
    msg.set_content(body)

    if SMTP_PORT == 465:
        # SSL
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)

def build_body(rows):
    if not rows:
        return "No stocks today."
    lines = []
    for i, (sym, score) in enumerate(rows, start=1):
        lines.append(f"{i}. {sym} — score={score:.6f}")
    return "\n".join(lines)

def main():
    today = datetime.now().date().isoformat()
    rows = get_top_scores(limit=5)
    subject = f"Top 5 stocks — {today}"
    body = build_body(rows)
    payload = {"min_score": MIN_SCORE, "count": len(rows), "rows": rows}
    body = body + "\n\n" + json.dumps(payload)
    send_email(subject, body)
    print("Email sent; rows:", len(rows))

if __name__ == "__main__":
    main()
