#!/usr/bin/env python
#
#
#  Copyright (c) 2024 by Bryan Murphy
#
# This file contains database table creation information. Converted for MySQL.
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
import pymysql


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
stock_db_info = ''' CREATE TABLE IF NOT EXISTS %s (
    FID            INT AUTO_INCREMENT PRIMARY KEY,
    SYMBOL          VARCHAR(32)    NOT NULL,
    LNAME           VARCHAR(255)   NOT NULL,
    GICSSector      VARCHAR(128)   NOT NULL,
    GICSSUB         VARCHAR(128)   NOT NULL,
    DATEADDED       DATE,
    CIK             VARCHAR(64)    NOT NULL,
    FOUNDED         DATE,
    ACTIVE          TINYINT DEFAULT 1
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
''' % (stock_db_tables['Info'])

# His : stockhistory table
stock_db_history = ''' CREATE TABLE IF NOT EXISTS %s (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        hdate       DATE    NOT NULL,
        symbol      VARCHAR(32)    NOT NULL,
        open        DOUBLE    NOT NULL,
        low         DOUBLE    NOT NULL,
        high        DOUBLE    NOT NULL,
        close       DOUBLE    NOT NULL,
        adjclose    DOUBLE    NOT NULL,
        volume      BIGINT    NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''' % (stock_db_tables['His'])

# Accnt : stock accounts table
# Can test with different accounts using different strategies
stock_db_account = ''' CREATE TABLE IF NOT EXISTS %s (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        acntname    VARCHAR(128)    NOT NULL,
        acntnumber  VARCHAR(64)     NOT NULL,
        cash        DOUBLE DEFAULT 0.0,
        Owner       VARCHAR(128)    NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''' % (stock_db_tables['Accnt'])

# Table to hold the results of tech analysis calculations
# The important parts are:
# Paramid - which paramid was use in the calculation
# mgrversion - the version assigned to the calculation routine
# Symbol, date
# The result of the calculation. Unless the version of the routine changes
# This data is static and used later by a buy routine (not defined yet)
# # 
stock_db_calcrslts = ''' CREATE TABLE IF NOT EXISTS %s (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        paramid     BIGINT    NOT NULL,
        symbol      VARCHAR(32)    NOT NULL,
        ldate       DATE    NOT NULL,
        mgrversion  DOUBLE    NOT NULL,
        rslt        DOUBLE    NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''' % (stock_db_tables['Calcrslts'])

# Params : parameters table
params_table = ''' CREATE TABLE IF NOT EXISTS %s(
    id INT AUTO_INCREMENT NOT NULL,
    sma_fast DOUBLE DEFAULT 9.0,
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
    """Create (optionally drop) a table using provided schema string.
    Returns True on success, False on failure."""
    try:
        with db.cursor() as cursor:
            if dropflag:
                sql_query = f"DROP TABLE IF EXISTS `{tablenm}`"
                logging.info("Dropping table: %s", sql_query)
                cursor.execute(sql_query)

            sql_query = tableschm
            logging.info("About to create a new table: %s", tablenm)
            logging.debug("Table Schema is %s", tableschm)
            cursor.execute(sql_query)
        db.commit()
        return True
    except pymysql.Error as e:
        logging.error("Error initializing table %s: %s", tablenm, e)
        try:
            db.rollback()
        except Exception:
            pass
        return False

# For the acccount table. Create the table and add several defaults
def accnt_default_data(db, table):
    # Create insert query for low_account, mid_account, and high_account
    rows = [
        ('Account_low', '123456', 1500.00, 'Kids'),
        ('Account_mid', '456789', 25000.00, 'Mom'),
        ('Account_high', '9876543', 100000.00, 'Dad')
    ]
    try:
        with db.cursor() as cnct:
            sqlq = f"INSERT INTO `{table}` (ACNTNAME, ACNTNUMBER, CASH, OWNER) VALUES (%s, %s, %s, %s)"
            cnct.executemany(sqlq, rows)
        db.commit()
    except pymysql.Error as error:
        logging.error("Failed to insert default account data into %s: %s", table, error)

def getstocklist(dbconn):
    qry = f"SELECT SYMBOL FROM `{stock_db_tables['Info']}` WHERE ACTIVE=1"
    try:
        with dbconn.cursor() as dbcnct:
            dbcnct.execute(qry)
            rslts = dbcnct.fetchall()
        logging.debug(ic(rslts))
        return rslts
    except pymysql.Error as e:
        logging.error("Error fetching stock list: %s", e)
        return []

def main() -> None:
    logging.info("Starting database table creation")
    logging.info(f"Major version {majorversion}, Minor version {minorversion}.")
    droptable = False
    db = opendatabase()
    if db is None:
        logging.error("Cannot open database; aborting table creation")
        return
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
    if Initializedb(db, droptable, stock_db_tables['Calcrslts'], stock_db_calcrslts):
        logging.info(F"Table %s created.", stock_db_tables['Calcrslts'])
    else:
        logging.info("Well that didn't work. Table %s isn't ready.", stock_db_tables['Calcrslts'])
    closedatabase(db)


if __name__ == '__main__':
    main()