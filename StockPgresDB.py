#!/usr/bin/env python
#
#
#  Copyright (c) 2024 by Bryan Murphy
#
# This file contains postgress database information.
# The database login/ connection information
# A list of table names for reference.
# The table structures for each table.
#
# It can be referenced in other scripts so one location has the key info.
#
# if run seperately it will initialize the tables in the database.

#import psycopg2
import sys
from icecream import ic
from StockCommon import *


stock_db_tables = {
    'His': "stockhistory", # Stock data history. Stored locally instead of hitting Yahoo Finance constantly
    'Info': "stockinfo", # Basic stock info for the S&P 500
    'Accnt': "accounts", # Cash is part of each account
    'Params': "parameters", # Parameters to back test. In the current market enviornment which parameters make the most money?
    'Calcrslts': "calcuresults", # for each parameter line calculate the resutls and store for each stock. - include up to last date of calculation, Params ID
# Some are for future work
#    'Buy' : "buytable", # Table we should buy. Stays in the table until purchase. The stock then goes to open table.
#    'Sell' : "selltable", # We have reached a Sell point for the stock
#    'Open' : "openpositions", # Current open positions for all accounts
#    'Transactions' : "trans", # Log transactions
#    'TransHistory' : "Trans history" # After selling put the total into history, buy, sell, profit
}

# Info : stockinfo table
stock_db_info = ''' CREATE TABLE IF NOT EXISTS public.%s
        (FID            SERIAL  Primary key   NOT NULL,
        SYMBOL          TEXT    NOT NULL,
        LNAME           TEXT    NOT NULL, 
        GICSSector      TEXT    NOT NULL,
        GICSSUB         TEXT    NOT NULL,
        DATEADDED       DATE    NOT NULL,
        CIK             TEXT    NOT NULL,
        FOUNDED         DATE    NOT NULL,
        ACTIVE          INT     DEFAULT 1
        );
''' % (stock_db_tables['Info'])

# His : stockhistory table
stock_db_history = ''' CREATE TABLE IF NOT EXISTS public.%s 
        (id          serial  Primary Key NOT NULL,
        hdate       date    NOT NULL,
        symbol      text    NOT NULL,
        open        real    NOT NULL,
        low         real    NOT NULL,
        high        real    NOT NULL,
        close       real    NOT NULL,
        adjclose    real    NOT NULL,
        volume      real    NOT NULL
        );
    ''' % (stock_db_tables['His'])

# Accnt : stock accounts table
# Can test with different accounts using different strategies
stock_db_account = ''' CREATE TABLE IF NOT EXISTS public.%s 
        (id          serial  Primary Key NOT NULL,
        acntname    text    NOT NULL,
        acntnumber  text    NOT NULL,
        cash        real    DEFAULT 0.0,
        Owner       text    NOT NULL
        );
    ''' % (stock_db_tables['Accnt'])

# Table to hold the results of tech analysis calculations
# The important parts are:
# Paramid - which paramid was use in the calculation
# mgrversion - the version assigned to the calculation routine
# Symbol, date
# The result of the calculation. Unless the version of the routine changes
# This data is static and used later by a buy routine (not defined yet)
# # 
stock_db_calcrslts = ''' CREATE TABLE IF NOT EXISTS public.%s 
        (id          serial  Primary Key NOT NULL,
        paramid     bigint    NOT NULL,
        symbol      text    NOT NULL,
        ldate       date    NOT NULL,
        mgrversion  real    NOT NULL,
        rslt        real    NOT NULL
        );
    ''' % (stock_db_tables['Calcrslts'])

# Params : parameters table
params_table = ''' CREATE TABLE IF NOT EXISTS public.%s(
    id SERIAL NOT NULL,
    sma_fast real DEFAULT 9.0,
    sma_fast_mul real DEFAULT 1.0,
    sma_slow real DEFAULT 14.0,
    sma_slow_mul real DEFAULT 1.0,
    ema_fast real DEFAULT 1.0,
    ema_fast_mul real DEFAULT 1.0,
    ema_slow real DEFAULT 14.0,
    ema_slow_mul real DEFAULT 1.0,
    momentum_n real DEFAULT 14.0,
    momentum_mul real DEFAULT 1.0,
    macd_fast real DEFAULT 12.0,
    macd_slow real DEFAULT 26.0,
    macd_mul real DEFAULT 1.0,
    macd_signal real DEFAULT 9.0,
    macd_sig_mul real DEFAULT 1.0,
    bb_n real DEFAULT 20.0,
    bb_sigma real DEFAULT 2.0,
    bb_mul real DEFAULT 1.0,
    rsi_n real DEFAULT 14.0,
    rsi_mul real DEFAULT 1.0,
    stoch_n real DEFAULT 20.0,
    stoch_mul real DEFAULT 1.0,
    ttmsq_mul real DEFAULT 5.0,
    PRIMARY KEY(id)
);
'''  % (stock_db_tables['Params'])

def Initializedb(db, dropflag, tablenm, tableschm) -> bool:
    cursor = db.cursor()
    rflag = False # only set to True if it works.

    if dropflag:
        sql_query = "DROP TABLE IF EXISTS %s" % tablenm
        logging.info(f"Dropping table: {sql_query}")
        r = cursor.execute(sql_query)
        if r == 'NULL':
            logging.error(f"Didn't erase the table {tablenm}")
    # Now create the table
    sql_query = tableschm
    logging.info(f"About to create a new table: {tablenm}")
    logging.info(f"Table Schema is {tableschm}")
    r = cursor.execute(sql_query)
    if r != 'NULL':
        rflag = True
    db.commit()
    return(rflag)

# For the acccount table. Create the table and add several defaults
def accnt_default_data(db, table):
    #Create insert query for low_account, mid_account, and high_account
    cnct = db.cursor()
    cash = 1500.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_low', '123456', %s, 'Kids')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        logging.error("Failed to execute the insert query", error)
    cash = 25000.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_mid', '456789', %s, 'Mom')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        logging.error("Failed to execute the insert query", error)
    cash = 100000.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_high', '9876543', %s, 'Dad')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        logging.error("Failed to execute the insert query", error)

def getstocklist(dbconn):
    dbcnct = dbconn.cursor()
    qry = "SELECT SYMBOL FROM %s WHERE ACTIVE=1" % (stock_db_tables['Info'])
    dbcnct.execute(qry)
    rslts = dbcnct.fetchall()
    logging.debug(ic(rslts))
    return(rslts)

def main() -> None:
    logging.info("Starting database table creation")
    logging.info(f"Major version {majorversion}, Minor version {minorversion}.")
    droptable = False
    db = opendatabase()
    #Change privalage to public teamplate to create tables
    dbcur = db.cursor()
    sql = f"GRANT ALL ON ALL TABLES IN SCHEMA public to {stock_db_params['user']}"
    dbcur.execute(sql)
    if Initializedb(db, droptable, stock_db_tables['Info'], stock_db_info):
        logging.info(F"Table {stock_db_tables['Info']} created.")
    else:
        logging.info(f"Well that didn't work. Table {stock_db_tables['Info']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['His'], stock_db_history):
        logging.info(F"Table {stock_db_tables['His']} created.")
    else:
        logging.info(f"Well that didn't work. Table {stock_db_tables['His']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Accnt'], stock_db_account):
        logging.info(F"Table {stock_db_tables['Accnt']} created.")
        accnt_default_data(db,stock_db_tables['Accnt']) # Load some initial data into this table
    else:
        logging.info(f"Well that didn't work. Table {stock_db_tables['Accnt']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Params'], params_table):
        logging.info(F"Table {stock_db_tables['Params']} created.")
    else:
        logging.info(f"Well that didn't work. Table {stock_db_tables['Params']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Calrslts'], stock_db_calcrslts):
        logging.info(F"Table {stock_db_tables['Calrslts']} created.")
    else:
        logging.info(f"Well that didn't work. Table {stock_db_tables['Calrslts']} isn't ready.")


if __name__ == '__main__':
    main()