#!/usr/bin/env python3
"""
Compute AI scores per stock using historical data in DB and store in calcuresults.
"""
import logging
import os
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import joblib
import json
from datetime import datetime

from StockCommon import opendatabase, majorversion

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def compute_features(df):
    df = df.sort_values('hdate').copy()
    df['close'] = df['close'].astype(float)
    df['ret_1'] = df['close'].pct_change(1)
    df['ret_3'] = df['close'].pct_change(3)
    df['ret_5'] = df['close'].pct_change(5)
    df['sma_5'] = df['close'].rolling(5).mean()
    df['sma_10'] = df['close'].rolling(10).mean()
    df['sma_ratio'] = df['sma_5'] / df['sma_10']
    df['vol_10'] = df['ret_1'].rolling(10).std()
    # RSI
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / (roll_down + 1e-9)
    df['rsi'] = 100.0 - (100.0 / (1.0 + rs))
    return df


def build_dataset(conn):
    qry = "SELECT hdate, symbol, open, high, low, close, adjclose, volume FROM stockhistory"
    # Use cursor fetch to avoid pandas DBAPI quirks with pymysql connections
    with conn.cursor() as cur:
        cur.execute(qry)
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if 'hdate' in df.columns:
        df['hdate'] = pd.to_datetime(df['hdate'])
    if df.empty:
        logging.error("No historical data found in stockhistory table")
        return pd.DataFrame()
    # If table contains header-like string rows, drop them
    if 'symbol' in df.columns:
        df = df[~df['symbol'].astype(str).str.lower().isin(['symbol', ''])]
    if 'close' in df.columns:
        df = df[~df['close'].astype(str).str.lower().isin(['close', ''])]
    # Coerce numeric columns
    for col in ['open', 'high', 'low', 'close', 'adjclose', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['hdate', 'symbol', 'close'])
    # Feature generation per symbol
    parts = []
    for sym, g in df.groupby('symbol'):
        gf = compute_features(g.rename(columns={'hdate': 'hdate'}))
        gf['symbol'] = sym
        parts.append(gf)
    allf = pd.concat(parts, ignore_index=True)
    # Label: future 5-day return
    allf['future_ret_5'] = allf.groupby('symbol')['close'].shift(-5) / allf['close'] - 1.0
    allf['label'] = (allf['future_ret_5'] > 0).astype(int)
    return allf


def train_model(df, feature_cols):
    tr = df.dropna(subset=feature_cols + ['label']).copy()
    if tr.empty:
        logging.error("No training rows after feature extraction")
        return None
    X = tr[feature_cols].astype(float)
    y = tr['label'].astype(int)
    # split for selection
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    candidates = {
        'RandomForest': RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1),
        'GradientBoosting': GradientBoostingClassifier(n_estimators=200, random_state=42)
    }
    best_model = None
    best_score = -1.0
    best_name = None
    for name, mdl in candidates.items():
        logging.info('Training %s', name)
        mdl.fit(X_train, y_train)
        if hasattr(mdl, 'predict_proba'):
            probs = mdl.predict_proba(X_val)[:, 1]
        else:
            probs = mdl.decision_function(X_val)
        try:
            auc = roc_auc_score(y_val, probs)
        except Exception:
            auc = 0.0
        logging.info('%s AUC=%.4f', name, auc)
        if auc > best_score:
            best_score = auc
            best_model = mdl
            best_name = name
    # persist best model and metadata
    os.makedirs('models', exist_ok=True)
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    model_path = f'models/best_model_{ts}_{best_name}.joblib'
    meta = {
        'model_name': best_name,
        'auc': float(best_score),
        'feature_cols': feature_cols,
        'timestamp_utc': ts
    }
    try:
        joblib.dump(best_model, model_path)
        with open('models/metadata.json', 'w') as f:
            json.dump(meta, f, indent=2)
        logging.info('Saved best model %s (AUC=%.4f) to %s', best_name, best_score, model_path)
    except Exception as e:
        logging.warning('Could not persist model: %s', e)
    return best_model


def score_latest_and_store(conn, model, df, feature_cols, paramid=1, mgrversion=majorversion):
    # For each symbol take the latest date with features
    latest = df.dropna(subset=feature_cols).sort_values('hdate').groupby('symbol').tail(1)
    if latest.empty:
        logging.info("No latest rows to score")
        return
    X = latest[feature_cols].astype(float)
    probs = model.predict_proba(X)[:, 1]
    rows = []
    # Build mapping of index to prob because latest.index may not start at 0
    for i, (idx, row) in enumerate(latest.iterrows()):
        rows.append((paramid, row['symbol'], row['hdate'].date(), mgrversion, float(probs[i])))
    # Insert into calcuresults
    insert_q = "INSERT INTO calcuresults (paramid, symbol, ldate, mgrversion, rslt) VALUES (%s, %s, %s, %s, %s)"
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_q, rows)
        conn.commit()
        logging.info("Inserted %d score rows into calcuresults", len(rows))
    except Exception as e:
        logging.error("Failed to insert calcuresults: %s", e)


def score_all_dates_and_store(conn, model, df, feature_cols, paramid=1, mgrversion=majorversion):
    # df expected to have features computed. Loop over unique dates and score per date.
    dates = sorted(df['hdate'].dt.date.unique())
    insert_q = "INSERT INTO calcuresults (paramid, symbol, ldate, mgrversion, rslt) VALUES (%s, %s, %s, %s, %s)"
    delete_q = "DELETE FROM calcuresults WHERE ldate = %s"
    total = 0
    for d in dates:
        day_rows = df[df['hdate'].dt.date == d].dropna(subset=feature_cols + ['symbol'])
        if day_rows.empty:
            continue
        X = day_rows[feature_cols].astype(float)
        try:
            probs = model.predict_proba(X)[:, 1]
        except Exception:
            probs = model.predict(X)
        rows = []
        for i, (idx, row) in enumerate(day_rows.iterrows()):
            rows.append((paramid, row['symbol'], d, mgrversion, float(probs[i])))
        try:
            with conn.cursor() as cur:
                cur.execute(delete_q, (d,))
                if rows:
                    cur.executemany(insert_q, rows)
            conn.commit()
            total += len(rows)
        except Exception as e:
            logging.error('Failed to insert calcuresults for %s: %s', d, e)
    logging.info('Inserted %d total score rows across %d dates', total, len(dates))


def main():
    conn = opendatabase()
    if conn is None:
        return
    df = build_dataset(conn)
    if df.empty:
        conn.close()
        return
    feature_cols = ['ret_1', 'ret_3', 'sma_ratio', 'vol_10', 'rsi']
    model = train_model(df, feature_cols)
    if model is None:
        conn.close()
        return
    # persist model
    try:
        joblib.dump(model, 'model.joblib')
        logging.info('Model saved to model.joblib')
    except Exception as e:
        logging.warning('Could not save model: %s', e)
    # allow backfill mode to score every date
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--backfill', action='store_true', help='Score all historical dates and insert into calcuresults')
    args, _ = parser.parse_known_args()
    if args.backfill:
        score_all_dates_and_store(conn, model, df, feature_cols)
    else:
        score_latest_and_store(conn, model, df, feature_cols)
    conn.close()


if __name__ == '__main__':
    main()
