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
#from yahoofinancials import YahooFinancials
# Need to have simple arguments
# Run full download, all history for the last two years
# Run daily, just the last day's data
# Get the last week of data
# For each type check the date to make sure it is not already in the database before we add the data.
import argparse
import numpy as np
import pandas as pd
#import requests 
#import sqlite3
#import os
#import math
#from pandas_datareader import data as pdr
import sys
#import json
from icecream import ic
from datetime import datetime
#from datetime import timedelta
import time
import calendar
import psycopg2
import logging

from StockPgresDB import opendatabase, Initializedb, stock_db_params, stock_db_tables, stock_db_info



# Do I wipe the database table while debugging
WipeDB = False
logging.basicConfig(
    level=logging.INFO,
    #filename="SDdownload.log",
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.debug("This is a debug message.")
logging.info("This is an info message.")
logging.warning("This is a warning message.")
logging.error("This is an error message.")
logging.critical("This is a critical message.")


def GetStockData(symboll, sddate='2024-01-01', eddate=datetime.today().strftime('%Y-%m-%d')):
    #edate = datetime.today().strftime('%Y-%m-%d')
    #print("Starting GetStockData")
    ic(symboll, sddate, eddate)
    stk_df = yf.download(symboll, sddate, eddate)
    logging.debug(stk_df.head())
    return(stk_df)

def getstocklist(dbconn):
    dbcnct = dbconn.cursor()
    qry = "SELECT SYMBOL FROM %s WHERE ACTIVE=1" % (stock_db_tables['Info'])
    dbcnct.execute(qry)
    rslts = dbcnct.fetchall()
    logging.debug(ic(rslts))
    return(rslts)

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
    return("weekly")

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
def inhistorycheck(conn, stock, sdata):
    # Returns True or False

    qry = "SELECT * FROM %s WHERE HDATE='%s' AND SYMBOL='%s'" % (stock_db_tables['His'], sdata[0], stock)
    logging.debug(ic(qry))
    cnct = conn.cursor()
    cnct.execute(qry)
    if(cnct.fetchone()):
        return(True)
    else:
        return(False)

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



def main():
    # Open database stockinfo for the symbols
    # Read in the symbols then get the history.
    # 
    # 
    logging.info(f"Starting {__name__}")
    # Use WipeDB = True to wiped database each time to make it clean.
    # Use WipeDB = False when adding data. 
    stockdb = opendatabase()
    Cursr = stockdb.cursor()

    # Init argparser then call function to setup args
    parser = argparse.ArgumentParser(description='Utility to update stock history data in stockdata database')
    tframe = args(parser)
    #ic(parser, tframe)
    #Default to daily if nothing else
    ############################
    #
    #
    #
    #tframe = "weekly"
    tday = datetime.today()
    if tframe == "daily":
        print("Daily update")
        starttime = daybefore(tday)
        #print("timedelta change.")
        #starttime = datetime.deltatime(day = -1)
        logging.debug(ic(starttime))
    elif tframe == "weekly":
        print("Weekly update")
        starttime = weekbefore(tday)
        # print("timedelta weekly")
        #starttime = datetime.timedelta(week = -1)
        logging.debug(ic(starttime))
    else:
        # If not daily or weekly then do full download. That means two years from today.
        print("Full update")
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
        Symbol = str(smb)
        Symbol = Symbol.strip("',()")
        Symbol = Symbol.replace(".","-") # The table has stocks with period, but yahoo needs a dash. Replace period in symbol with dash.
        #Symbol = "BRK.B"
        logging.debug(ic(Symbol, starttime, endtime))
        #stock_df = yf.download(Symbol, start=starttime, end=endtime, rounding=True)
        stock_df = gethisdata(Symbol, starttime, endtime)
        #stock_df = gethisdata("xxx", starttime, endtime)
        logging.info(ic("Did I get back an empty?", stock_df))
        if stock_df.empty:
            print(f"Symbol {smb} did not return data.")
            logging.info(f"Symbol {smb} did not return data.")
            continue
        logging.debug(ic(stock_df))
        stock_df.reset_index(inplace=True)
        stock_df['Date'] = stock_df['Date'].dt.strftime('%Y-%m-%d')
        logging.info(ic(stock_df['Date']))
        logging.info(ic(len(stock_df)))
        logging.info(ic(stock_df))
        cnct = stockdb.cursor()
        for t in range(len(stock_df)):
            logging.debug(ic(t, Stkhisd))
            if inhistorycheck(stockdb, Symbol, stock_df.iloc[t]):
                logging.debug(f"{Symbol} is in the database for date {stock_df.iloc[t]}")
            else:
                #print("Not in the database.")
                qry = "INSERT INTO %s (HDATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, AdjClose, VOLUME) VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s)" %  (stock_db_tables['His'], stock_df.iloc[t][0], Symbol, stock_df.iloc[t][1], stock_df.iloc[t][2], stock_df.iloc[t][3], stock_df.iloc[t][4], stock_df.iloc[t][5], stock_df.iloc[t][6])
                logging.debug(ic(qry))
                try:
                    cnct.execute(qry)
                    Stkhisd += 1
                except psycopg2.Error as error:
                    print("Failed to execute the insert query", error)
                    logging.error("Failed to execute the insert query", error)
        Stkcount += 1
        Totalcnt += Stkhisd  
        Stkhisd = 0
        stockdb.commit()
        #if debug:
        #    if Symbol != "MMM":
        #        break

    logging.info('Results')
    logging.info(f"Total stocks processed is {Stkcount}")
    logging.info(f"Total data inserts is {Totalcnt}")
    print(f"Total stocks processed is {Stkcount}")
    print(f"Total data inserts is {Totalcnt}")

    # Close info database We are done    
    stockdb.commit()
    stockdb.close()


if __name__ == '__main__':
    print("Getting Stock Historical Data")
    sys.exit(main())  

    
