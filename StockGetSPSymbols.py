#!/usr/bin/env python
#
#
#  Copyright (c) 2024, by Bryan Murphy
#
#  license: GNU LGPL

# This utility will download the S&P list from Wikipedia.org and puts into an postgresql database so all local scripts can access the list.
##
##
# The simple table of the S&P 500 is setup to wipe and restart. Other tables with have history and other
# values. The table is wiped and recreated to eliminate possibility of duplicates

import bs4 as bs # BeautifulSoup used to get a table from wiki page
import requests # Used to get the webpage, url
from datetime import datetime
import psycopg2
from icecream import ic # Used to print debug messages
from StockPgresDB import opendatabase, Initializedb, stock_db_params, stock_db_tables, stock_db_info
from Versions import MajorVersion, MinorVersion, debug
import logging

# Set level for ic debug statements
# zero means none
# 1 (icdebug > 0) only least important
# 2 Prcedure/loop start plus level 1
# 3 Everything
icdebug = 1
logging.basicConfig(filename="SDdownload.log")
logger = logging.getLogger(__name__)

def GetStockSymbols():
    # Get S&P500 Tickers from Wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = bs.BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "wikitable sortable"})
    tickers = []

    for row in table.findAll("tr")[1:]:
        ticker = row.findAll("td")[0].text.strip()
        # Change any '.' to '-' like BRK.B to BRK-B
        ticker = ticker.replace(".", "-")
        stkname = row.findAll("td")[1].text.strip()
        GICSSector = row.findAll("td")[2].text.strip()
        GICSSub = row.findAll("td")[3].text.strip()
        DateAdded = row.findAll("td")[5].text.strip()
        CIK = row.findAll("td")[6].text.strip()
        Founded = row.findAll("td")[7].text.strip()
        Stklist = [ticker, stkname, GICSSector, GICSSub, DateAdded, CIK, Founded]
        tickers.append(Stklist)
    # Add SPY to the list
    ticker = "SPY"
    stkname = "SP500"
    GICSSector = "Industrials"
    GICSSub = "Industrials"
    DateAdded = "2000-01-01"
    CIK = "00000001"
    Founded = "1888-01-01"
    Stklist = [ticker, stkname, GICSSector, GICSSub, DateAdded, CIK, Founded]
    tickers.append(Stklist)
#    ic(tickers)
    return(tickers)


if __name__ == "__main__":
    #Open Database
    dbconn = opendatabase()
    #
    #  Clear the table so there are no duplicates
    #
    Initializedb(dbconn, True, stock_db_tables['Info'], stock_db_info)
    stocks = GetStockSymbols()
    dbcur = dbconn.cursor()
    logger.info('Starting Get Stock Symbols')
    # Insert the stocks into the database
    if icdebug > 1:
        print("Starting loop to add stocks.")
    for simb in range(len(stocks)):
        # simb is one of the stocks from the table to put into the table
        if icdebug > 1:
            ic(stocks[simb])
        # Process the long name to remove apostrophies
        symb = stocks[simb][0]
        symb = symb.replace(".","-") # The table has stocks with period, but yahoo needs a dash. Replace period in symbol with dash.
        lnm = stocks[simb][1]
        if icdebug > 2:
            ic(lnm)
        lnm = lnm.replace("'", "")
        if icdebug > 2:
            ic(lnm)
        # Date added to the S&P 500 put into the correct format
        dadd = datetime.strptime(stocks[simb][4], "%Y-%m-%d")
        if icdebug > 1:
            ic(dadd)
        # Year company founded
        adyr = stocks[simb][6][0:4]
        adyr = datetime.strptime(adyr,"%Y")
        if icdebug > 2:
            ic(adyr)
        qry = "INSERT INTO %s (SYMBOL, LNAME, GICSSector, GICSSUB, DATEADDED, CIK, FOUNDED, ACTIVE)" % (stock_db_tables['Info'])
        if icdebug > 2:
            ic(qry)
        qry = qry + " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (stocks[simb][0], lnm, stocks[simb][2], stocks[simb][3], dadd, stocks[simb][5], adyr, 1)
        if icdebug > 2:
            ic(qry)
        dbcur.execute(qry)
        dbconn.commit()

dbconn.close()
print(f"Finished and processed {len(stocks)} stock records")



    