#Python 3.10
#
#
#  Copyright (c) 2024, Bryan Murphy
#
'''
    Notes on database
    For remote access to the mysql datbase you need to modify files and firewall
    
    In pg_hba.conf file
    Add the ip address of the machines that will access the database

    Firewall
    sudo ufw allow 5432/tcp

    Create the database on the machine
    Create the user for the database
    Align the information in the section below

    Grant the user access to the public schema to create tables.
    GRANT ALL ON ALL TABLES IN SCHEMA public to user


'''


import pymysql.cursors
import logging

logging.basicConfig(
    level=logging.DEBUG,
    #filename="SDdownload.log",
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

majorversion = 1.0
minorversion = 2.0
Change_Date = "2024-06-28"

recalculateall = False

# Parameters to log into the database.
# The database must already be setup and allowing connections from your device
#
# dbname -> stockanalysis is working database
# dbname -> stockanaltest is testing database table working
#
#
stock_db_params = {
# Postgres database Root password - 'Pgres#ma$ter'
    'database': 'stockanal',
    'user': 'stockfrk',
    'password': 'Stk$freak#pass7',
    'host': 'localhost',  # Change this if your database is hosted elsewhere
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor,
    'charset': 'utf8mb4'
}


def opendatabase():
    """Open and return a pymysql connection, or None on failure."""
    try:
        logging.info("Connecting to DB %s at %s:%s",
                     stock_db_params.get('database'),
                     stock_db_params.get('host'),
                     stock_db_params.get('port'))
        conn = pymysql.connect(**stock_db_params)
        try:
            conn.autocommit(True)
        except Exception:
            # Some pymysql versions may expose autocommit differently; ignore if not supported
            pass
        logging.info("Database connection established")
        return conn
    except pymysql.Error as e:
        logging.error("Failed to connect to database: %s", e)
        return None




def get_table_col_names(dbconn, table_str):
    print(f"Columns for table {table_str}")
    col_names = []
    if dbconn is None:
        logging.error("get_table_col_names called with None dbconn")
        return col_names

    # Validate table identifier to avoid SQL injection. Allow schema.table using dots.
    import re
    if not re.match(r'^[A-Za-z0-9_\.]+$', table_str):
        logging.error("Invalid table name: %s", table_str)
        return col_names

    # Quote each identifier part with backticks (safe for MySQL identifiers)
    parts = table_str.split('.')
    quoted = '.'.join([f"`{p}`" for p in parts])

    try:
        with dbconn.cursor() as cur:
            cur.execute(f"SELECT * FROM {quoted} LIMIT 0")
            if cur.description:
                for desc in cur.description:
                    col_names.append(desc[0])
    except pymysql.Error as e:
        logging.error("Error fetching columns for %s: %s", table_str, e)
    except Exception as e:
        logging.error("Unexpected error in get_table_col_names: %s", e)
    return col_names


def closedatabase(conn):
    """Close a pymysql connection safely."""
    if conn is None:
        return
    try:
        conn.close()
        logging.info("Database connection closed")
    except Exception as e:
        logging.error("Error closing database connection: %s", e)

def main() -> None:
    logging.info("Starting the program")
    logging.info("Version info: %s.%s", majorversion, minorversion)
    logging.info("Change Date: %s", Change_Date)

    db = opendatabase()
    closedatabase(db)

if __name__ == '__main__':
    main()