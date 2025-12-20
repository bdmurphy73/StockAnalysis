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
from icecream import ic # Used to print debug messages
from datetime import datetime

from StockPgresDB import *
from StockCommon import *

def GetStockSymbols():
    # Get S&P500 Tickers from Wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logging.error("Failed to fetch S&P 500 list: HTTP %s", response.status_code)
        return []
    soup = bs.BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "wikitable sortable"})
    if table is None:
        logging.error("Could not find S&P 500 table on the page")
        return []
    tickers = []

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        # Need at least symbol and name
        if len(cells) < 2:
            continue
        ticker = cells[0].text.strip()
        # Change any '.' to '-' like BRK.B to BRK-B
        ticker = ticker.replace(".", "-")
        stkname = cells[1].text.strip() if len(cells) > 1 else ""
        GICSSector = cells[2].text.strip() if len(cells) > 2 else ""
        GICSSub = cells[3].text.strip() if len(cells) > 3 else ""
        DateAdded = cells[5].text.strip() if len(cells) > 5 else ""
        CIK = cells[6].text.strip() if len(cells) > 6 else ""
        Founded = cells[7].text.strip() if len(cells) > 7 else ""
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
    return(tickers)


if __name__ == "__main__":
    #Open Database
    #bob = "Testing" # trying to figure out why ic function didn't work below.
    #ic(bob)
    dbconn = opendatabase()
    if dbconn is None:
        logging.error("Could not open database; aborting")
        sys.exit(1)

    # Clear the table so there are no duplicates
    Initializedb(dbconn, True, stock_db_tables['Info'], stock_db_info) # Function in file StockPgresDB.py

    stocks = GetStockSymbols()
    logging.info("Starting Get Stock Symbols")
    # Insert the stocks into the database
    logging.info("Starting loop to add stocks.")

    insert_sql = f"INSERT INTO `{stock_db_tables['Info']}` (SYMBOL, LNAME, GICSSector, GICSSUB, DATEADDED, CIK, FOUNDED, ACTIVE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    for simb, stock in enumerate(stocks):
        logging.debug(ic(simb))
        symb = stock[0].replace('.', '-')
        lnm = stock[1].replace("'", "")

        # Date added to the S&P 500 put into the correct format
        try:
            dadd = datetime.strptime(stock[4], "%Y-%m-%d").date()
        except Exception:
            dadd = None
            logging.debug("Failed to parse DateAdded for %s: %s", symb, stock[4])

        # Year company founded
        try:
            adyr = stock[6][0:4]
            adyr = datetime.strptime(adyr, "%Y").date()
        except Exception:
            adyr = None
            logging.debug("Failed to parse Founded year for %s: %s", symb, stock[6])

        params = (symb, lnm, stock[2], stock[3], dadd, stock[5], adyr, 1)

        try:
            with dbconn.cursor() as dbcur:
                dbcur.execute(insert_sql, params)
            dbconn.commit()
        except pymysql.Error as e:
            logging.error("Failed to insert %s: %s", symb, e)

    closedatabase(dbconn)
    logging.info("Finished and processed %d stock records", len(stocks))



    