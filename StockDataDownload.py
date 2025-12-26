#!python3
#
#
#  Copyright (c) 2024, by Bryan Murphy
#
#  license: GNU LGPL
#

# Module to get historical stock data.
# Get a daily, weekly or full update update
# For each date check to make sure it is not already in the database before we add the data.
# Store it in the database for other scripts to use so we aren't constantly calling the yahoo servers.

# Hitory Database info
# HistoryTable_tbl = "(id int primary key autoincrement unique, hisdate text, symbol text, open real, low real, high real, close real, volume real)"
# DbaseName = StockHistory.db TableName = stockhistory
# cur.execute("DROP TABLE IF EXISTS stockhistory")
# cur.execute('''CREATE TABLE IF NOT EXISTS stockhistory (id int primary key autoincrement unique, hisdate text, symbol text, open real, low real, high real, close real, volume real)''')


import yfinance as yf
import argparse
import numpy as np
import pandas as pd
from icecream import ic

import sys
#import json
from datetime import datetime
import time
import calendar

from StockPgresDB import *
from StockCommon import *
import pymysql

# Do I wipe the database table while debugging
WipeDB = False

def GetStockData(symboll, sddate='2024-01-01', eddate=datetime.today().strftime('%Y-%m-%d')):
    #edate = datetime.today().strftime('%Y-%m-%d')
    #print("Starting GetStockData")
    logging.debug(ic(symboll, sddate, eddate))
    stk_df = yf.download(symboll, sddate, eddate)
    logging.debug(stk_df.head())
    return(stk_df)

def getstocklist(dbconn):
    try:
        with dbconn.cursor() as cur:
            qry = f"SELECT SYMBOL FROM `{stock_db_tables['Info']}` WHERE ACTIVE=1"
            cur.execute(qry)
            rows = cur.fetchall()
        # rows may be list of dicts depending on cursorclass; normalize to list of strings
        syms = []
        for r in rows:
            if isinstance(r, dict):
                syms.append(r.get('SYMBOL'))
            else:
                # tuple or single value
                syms.append(r[0])
        logging.debug(ic(syms))
        return syms
    except pymysql.Error as e:
        logging.error("Error fetching stock list: %s", e)
        return []

#############################################################
# Parse args with argument a parser object
# Passed variable is a parser object
# Choices are:
#
# Full - download the last two years
# Weekly - download the last seven days
# Daily - download yesterday
#
# Return the argument string
def args(parser):
    # Setup the argparser options
    # One arg, can be full, week, daily
    parser.add_argument('timeframe', action='store', nargs='?', type=str, const='weekly', choices=['full', 'weekly', 'daily'], default='daily', help='One required argument, full, weekly, daily')
    timeframe = parser.parse_args()
    tframe = timeframe.timeframe
    tframe = tframe.lower()
    logging.debug(ic(tframe))
    # Force weekly <<<<--------------- Change this return(tframe)
    return(tframe)

##################################################
# Return the date one week before today.
# Account for end of month and end of year
#
# Does not care about weekends, holidays or anything else, only calendar days.
#
# Passed date string for today
# Returns seven days ago 
# 
def weekbefore(today):
    # week before today, which could be the previous month or year if today is the first of Jan 1st.
    day=today.day - 7 # if day = 1-6 it is the previous month
    month=today.month
    year=today.year
    if day < 1:
        if month == 1:
            year=today.year-1
            month=12
            day=31 - abs(day)
        else:
            month -= 1
            day = calendar.monthrange(year, month)[1] - abs(day)
    newday = datetime(int(year),int(month), int(day))
    return(newday.strftime('%Y-%m-%d'))

##################################################
# Return the date one day before today.
# Account for end of month and end of year
#
# Does not care about weekends, holidays or anything else, only calendar days.
#
# Passed date string for today
# Returns one day ago 
#    
def daybefore(today):
    # day before today, which could be the previous month or year if today is the first of Jan 1st.
    day=today.day
    month=today.month
    year=today.year
    if today.day == 1:
        if today.month == 1:
            year=today.year-1
            month=12
            day=31
        else:
            month=today.month-1
            day = calendar.monthrange(year, month)[1]
            logging.debug(ic(day))
    else:
        day=today.day-1
    newday = datetime(int(year),int(month), int(day))
    return(newday.strftime('%Y-%m-%d'))

##################################################
# Return the next day after a passed date.
# Account for end of month and end of year
#
# Does not care about weekends, holidays or anything else, only calendar days.
#
# Passed date string for the day you want to increment
# Returns the date for one day in the future 
# 
def nextday(today):
    tday = datetime.strptime(today, "%Y-%m-%d")
    day=tday.day
    month=tday.month
    year=tday.year
    #ic(day, month, year)
    lastday = calendar.monthrange(year,month)
    #ic(day, (day+1), lastday[1])
    if day+1 > lastday[1]:
        if month+1 > 12:
            year += 1
            month = 1
            day = 1
        else:
            day = 1
            month += 1
    else:
        day += 1
    nextday = datetime(int(year), int(month), int(day))
    nxday = nextday.strftime('%Y-%m-%d')
    return(nxday)

##################################################
# Is there data in the history database for the specified stock on the specified day
#
# Arguments
# Conn - database pointer
# stock - the stock symbol
# sdate - the date to check
# Returns:
# True if the database has the data
# False if the database does not have the data
#
def inhistorycheck(conn, stock, sdate):
    """Return True if history row exists for stock on sdate (YYYY-MM-DD)."""
    try:
        with conn.cursor() as cur:
            qry = f"SELECT 1 FROM `{stock_db_tables['His']}` WHERE HDATE=%s AND SYMBOL=%s LIMIT 1"
            cur.execute(qry, (sdate, stock))
            row = cur.fetchone()
        return bool(row)
    except pymysql.Error as e:
        logging.error("Error checking history for %s %s: %s", stock, sdate, e)
        return False

def gethisdata(Symbol, STart, ENd):
    #setup loop to try three times if there is an error.
    #Add a time delay
    Loopcount = 1
    stock_df = pd.DataFrame()
    while Loopcount <= 3:
        try:
            stock_df = yf.download(Symbol, start=STart, end=ENd, rounding=True)
        except:
            if Loopcount == 3:
                time.sleep(60) # If we are requesting too much YF may have an issue. Delay one second for the last try.
            continue
        else:
            Loopcount = 5
        finally:
            Loopcount += 1
    logging.info(ic("I tried to get data", Symbol, stock_df))
    return(stock_df)

def main() -> None:
    # Open database stockinfo for the symbols
    # Read in the symbols then get the history.
    # 
    # 
    logging.info(f"Starting {__name__}")
    # Use WipeDB = True to wiped database each time to make it clean.
    # Use WipeDB = False when adding data. 
    stockdb = opendatabase()
    #Cursr = stockdb.cursor()

    # Init argparser then call function to setup args
    parser = argparse.ArgumentParser(description='Utility to update stock history data in stockdata database')
    tframe = args(parser)

    tday = datetime.today()
    if tframe == "daily":
        logging.info("Daily update")
        starttime = daybefore(tday)
        #print("timedelta change.")
        #starttime = datetime.deltatime(day = -1)
        logging.debug(ic(starttime))
    elif tframe == "weekly":
        logging.info("Weekly update")
        starttime = weekbefore(tday)
        # print("timedelta weekly")
        #starttime = datetime.timedelta(week = -1)
        logging.debug(ic(starttime))
    else:
        # If not daily or weekly then do full download. That means two years from today.
        logging.info("Full update")
        starttime = datetime(int(tday.year-2), int(tday.month), int(tday.day)).strftime('%Y-%m-%d')
        logging.debug(ic(starttime))

    #starttime = args(parser)
    endtime = datetime.today().strftime('%Y-%m-%d')

    # Counts to display at the end
    # Stkcount - the number of stocks to process
    # Stkhisd - the number of days inserted for a stock
    # Totalcnt - The total of each Stkhisd summed
    Stkcount = 0
    Stkhisd = 0
    Totalcnt = 0
    
    logging.info(ic(endtime))
    # Read stock list and loop
    allsymbols = getstocklist(stockdb)
    for smb in allsymbols:
        logging.debug(ic(smb))
        Symbol = str(smb).strip()
        Symbol = Symbol.strip("',()")
        Symbol = Symbol.replace(".","-") # Yahoo uses dash instead of period in tickers
        logging.debug(ic(Symbol, starttime, endtime))
        stock_df = gethisdata(Symbol, starttime, endtime)
        logging.info(ic("Did I get back an empty?", stock_df))
        if stock_df.empty:
            print(f"Symbol {smb} did not return data.")
            logging.info(f"Symbol {smb} did not return data.")
            continue
        logging.debug(ic(stock_df))
        stock_df.reset_index(inplace=True)
        # Flatten MultiIndex columns produced by yfinance when a ticker is present
        if isinstance(stock_df.columns, pd.MultiIndex):
            newcols = []
            for col in stock_df.columns:
                if isinstance(col, tuple):
                    parts = [str(c).strip() for c in col if c and str(c).strip() != '']
                    newcols.append('_'.join(parts) if parts else '')
                else:
                    newcols.append(str(col))
            stock_df.columns = [c if c else f'col_{i}' for i, c in enumerate(newcols)]

        # Find a suitable date column (looks for column name containing 'date')
        date_col = None
        for c in stock_df.columns:
            if 'date' in str(c).lower():
                date_col = c
                break
        if date_col is None:
            date_col = stock_df.columns[0]

        # Ensure each cell in the date column is a scalar (not a Series) then format
        def _scalar_date(x):
            if isinstance(x, (pd.Series, list, tuple)):
                try:
                    return x.iloc[0] if isinstance(x, pd.Series) else x[0]
                except Exception:
                    return None
            return x

        stock_df[date_col] = stock_df[date_col].apply(_scalar_date)
        stock_df['Date'] = pd.to_datetime(stock_df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
        logging.debug(ic(stock_df['Date']))
        logging.debug(ic(len(stock_df)))
        logging.debug(ic(stock_df))
        # Insert rows using parameterized SQL
        insert_sql = f"INSERT INTO `{stock_db_tables['His']}` (HDATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, `AdjClose`, VOLUME) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        # Helper to find a column value by substring (handles flattened multiindex names)
        def _find_val(row, key_substr):
            ks = key_substr.lower()
            for c in row.index:
                if ks in str(c).lower():
                    val = row.get(c)
                    if isinstance(val, (pd.Series, list, tuple)):
                        try:
                            return val.iloc[0] if isinstance(val, pd.Series) else val[0]
                        except Exception:
                            return None
                    return val
            return None

        for _, row in stock_df.iterrows():
            logging.debug(ic(_, Stkhisd))
            date_str = row.get('Date')
            if inhistorycheck(stockdb, Symbol, date_str):
                logging.debug("%s is in the database for date %s", Symbol, date_str)
                continue
            # Map DataFrame columns to variables; handle flattened/multiindex column names
            open_v = _find_val(row, 'open')
            high_v = _find_val(row, 'high')
            low_v = _find_val(row, 'low')
            close_v = _find_val(row, 'close')
            adj_v = _find_val(row, 'adj') or _find_val(row, 'adjclose') or close_v
            vol_v = _find_val(row, 'volume')
            params = (date_str, Symbol, open_v, high_v, low_v, close_v, adj_v, vol_v)
            try:
                with stockdb.cursor() as cur:
                    cur.execute(insert_sql, params)
                Stkhisd += 1
            except pymysql.Error as error:
                logging.error("Failed to insert history for %s on %s: %s", Symbol, date_str, error)
        Stkcount += 1
        Totalcnt += Stkhisd  
        Stkhisd = 0
        stockdb.commit()

    logging.info('Results')
    logging.info(f"Total stocks processed is {Stkcount}")
    logging.info(f"Total data inserts is {Totalcnt}")
    print(f"Total stocks processed is {Stkcount}")
    print(f"Total data inserts is {Totalcnt}")

    # Close info database We are done
    stockdb.commit()
    closedatabase(stockdb)


if __name__ == '__main__':
    today = datetime.today().strftime('%Y-%m-%d')
    logging.info(f"Getting Stock Historical Data for {today}")
    main()  

    
