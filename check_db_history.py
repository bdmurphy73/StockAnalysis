#!/usr/bin/env python3
"""
Script to check the database and report when the stock history was last updated.
Shows the latest date in the stockhistory table and provides summary statistics.
"""

import sys
import logging
from datetime import datetime
from StockCommon import opendatabase, closedatabase
from StockPgresDB import stock_db_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def check_history_update_date():
    """Check the database for the last date the stock history was updated."""
    
    db = opendatabase()
    if db is None:
        logging.error("Cannot open database; aborting")
        return False
    
    try:
        with db.cursor() as cursor:
            # Get the most recent date in the history table
            query = f"SELECT MAX(hdate) as latest_date FROM `{stock_db_tables['His']}`"
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result and result.get('latest_date'):
                latest_date = result['latest_date']
                print("\n" + "="*60)
                print("DATABASE HISTORY UPDATE CHECK")
                print("="*60)
                print(f"Last history update date: {latest_date}")
                print(f"Table: {stock_db_tables['His']}")
                
                # Calculate days since last update
                from datetime import datetime, date
                today = date.today()
                days_since = (today - latest_date).days
                print(f"Days since last update: {days_since}")
                
                if days_since == 0:
                    print("Status: Updated TODAY ✓")
                elif days_since == 1:
                    print("Status: Updated YESTERDAY ✓")
                else:
                    print(f"Status: Last updated {days_since} days ago")
                
                # Get count of records
                query_count = f"SELECT COUNT(*) as total_records FROM `{stock_db_tables['His']}`"
                cursor.execute(query_count)
                count_result = cursor.fetchone()
                if count_result:
                    print(f"Total records in history: {count_result['total_records']:,}")
                
                # Get count of unique symbols
                query_symbols = f"SELECT COUNT(DISTINCT symbol) as unique_symbols FROM `{stock_db_tables['His']}`"
                cursor.execute(query_symbols)
                symbols_result = cursor.fetchone()
                if symbols_result:
                    print(f"Unique symbols in history: {symbols_result['unique_symbols']}")
                
                # Get earliest date in history
                query_min = f"SELECT MIN(hdate) as earliest_date FROM `{stock_db_tables['His']}`"
                cursor.execute(query_min)
                min_result = cursor.fetchone()
                if min_result and min_result.get('earliest_date'):
                    print(f"Earliest date in history: {min_result['earliest_date']}")
                
                print("="*60 + "\n")
                return True
            else:
                print("\n" + "="*60)
                print("DATABASE HISTORY CHECK")
                print("="*60)
                print(f"No data found in {stock_db_tables['His']} table")
                print("History table appears to be empty.")
                print("="*60 + "\n")
                return False
                
    except Exception as e:
        logging.error(f"Error checking database: {e}")
        return False
    finally:
        closedatabase(db)

if __name__ == '__main__':
    success = check_history_update_date()
    sys.exit(0 if success else 1)
