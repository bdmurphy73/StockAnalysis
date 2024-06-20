#!/usr/bin/env python
#
#
#  Copyright (c) 2024, created by Bryan Murphy
#
#  license: GNU LGPL

import psycopg2
import sys
#import icecream as ic
from StockCommon import getstocklist, get_table_col_names


stock_db_tables = {
    'His': "stockhistory", # Stock data history. Stored locally instead of hitting Yahoo Finance constantly
    'Info': "stockinfo", # Basic stock info for the S&P 500
    'Accnt': "accounts", # Cash is part of each account
    'Params': "parameters", # Parameters to back test. In the current market enviornment which parameters make the most money?
    'Calrslts': "calcuresults", # for each parameter line calculate the resutls and store for each stock. - include up to last date of calculation, Params ID
    'Buy' : "buytable", # Table we should buy. Stays in the table until purchase. The stock then goes to open table.
    'Sell' : "selltable", # We have reached a Sell point for the stock
    'Open' : "openpositions", # Current open positions for all accounts
    'Transactions' : "trans", # Log transactions
    'TransHistory' : "Trans history" # After selling put the total into history, buy, sell, profit
}

# Parameters to log into the database.
# The database must already be setup and allowing connections from you device
#
#
# Parameters to access the database
#
stock_db_params = {
# Postgres database Root password - 'Pgres#ma$ter'
    'dbname': 'stockanalysis',
    'user': 'stockfrk',
    'password': 'Frkpassword',
    'host': '192.168.1.1',  # Change this if your database is hosted elsewhere
    'port': 5432,  # Default PostgreSQL port
}

# Basic open database and provide the connection for others
def opendatabase():
    try:
        db = psycopg2.connect(**stock_db_params)
    except psycopg2.Error as e:
        print(e)
        return(False)
    print("Looks like we are connected to server %s and database %s" % (stock_db_params['host'], stock_db_params['dbname']))
    return(db)

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

# Table to hold the results of calculations with stock, params id, last calc date. 
# for a parameter set and historical dates the values won't change. 
# Can use this to store the backtest results for each day with each parameter id
# Then use the results to decide to buy/sell on each day
# Allows calculations to run indepdent of buy/sell testing
stock_db_calrslts = ''' CREATE TABLE IF NOT EXISTS public.%s 
        (id          serial  Primary Key NOT NULL,
        paramid     bigint    NOT NULL,
        acntnumber  text    NOT NULL,
        symbol      text    NOT NULL,
        ldate       date    NOT NULL,
        mgrversion  real    NOT NULL,
        rslt        real    NOT NULL,
        Owner       text    NOT NULL
        );
    ''' % (stock_db_tables['Calrslts'])

# Params : parameters table
params_table = ''' CREATE TABLE IF NOT EXISTS public.%s 
        (id          serial primary key NOT NULL,
        SMA_fast        real default 9.0,
        sma_fast_mul    real default 1.0,
        SMA_slow        real default 14.0,
        sma_slow_mul    real default 1.0,
        EMA_fast        real default 1.0,
        ema_fast_mul    real default 1.0,
        EMA_slow        real default 14.0,
        ema_slow_mul    real default 1.0,
        momentum_n      real default 14.0,
        momentum_mul    real default 1.0,
        macd_fast       real default 12.0,
        macd_slow       real default 26.0,
        macd_mul        real default 1.0,
        macd_signal_spd real default 9.0,
        macd_sig_mul    real default 1.0,
        macd_histagram  real default 2.0,
        bb_prd            real default 20.0,
        bb_sigma        real default 2.0,
        bb_mul          real default 1.0,
        keltn_prd       real default 20.0,
        profit_target   real default 0.10,
        loss_target     real default 0.05,
        time_limit      real default 5.0,
        roc_mul         real default 1.0,
        roc_n           real default 14.0,
        rsi_n           real default 14.0,
        rsi_mul         real default 1.0,
        stoch_n         real default 20.0,
        stoch_mul       real default 1.0
        );
'''  % (stock_db_tables['Params'])



def Initializedb(db, flag, tablenm, tableschm):
    cursor = db.cursor()
    rflag = False
    if flag:
        sql_query = "DROP TABLE IF EXISTS %s" % tablenm
        print(sql_query)
        r = cursor.execute(sql_query)
        if r == 'NULL':
            print(f"Didn't erase the table {tablenm}")

    # Now create the table
    sql_query = tableschm
    print("Do we create a new table?",tablenm)
    print(f"Schema is {tableschm}")
    r = cursor.execute(sql_query)
    if r != 'NULL':
        rflag = True
    db.commit()
    return(rflag)

def accnt_default_data(db, table):
    #Create insert query for low_account, mid_account, and high_account
    cnct = db.cursor()
    cash = 1500.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_low', '123456', %s, 'Kids')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        print("Failed to execute the insert query", error)
    cash = 25000.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_mid', '456789', %s, 'Mom')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        print("Failed to execute the insert query", error)
    cash = 100000.00
    sqlq = "INSERT INTO %s (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES ('Account_high', '9876543', %s, 'Dad')" % (table, cash)
    try:
        cnct.execute(sqlq)
    except psycopg2.Error as error:
        print("Failed to execute the insert query", error)


def main() -> int:
    """Echo the input arguments to standard output"""
    droptable = False
    db = opendatabase()
    if Initializedb(db, droptable, stock_db_tables['Info'], stock_db_info):
        print(F"Table {stock_db_tables['Info']} created.")
    else:
        print(f"Well that didn't work. Table {stock_db_tables['Info']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['His'], stock_db_history):
        print(F"Table {stock_db_tables['His']} created.")
    else:
        print(f"Well that didn't work. Table {stock_db_tables['His']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Accnt'], stock_db_account):
        print(F"Table {stock_db_tables['Accnt']} created.")
        accnt_default_data(db,stock_db_tables['Accnt']) # Load some initial data into this table
    else:
        print(f"Well that didn't work. Table {stock_db_tables['Accnt']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Params'], params_table):
        print(F"Table {stock_db_tables['Params']} created.")
    else:
        print(f"Well that didn't work. Table {stock_db_tables['Params']} isn't ready.")
    if Initializedb(db, droptable, stock_db_tables['Calrslts'], stock_db_calrslts):
        print(F"Table {stock_db_tables['Calrslts']} created.")
    else:
        print(f"Well that didn't work. Table {stock_db_tables['Calrslts']} isn't ready.")
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 
