#Python 3.10
#
#
#  Copyright (c) 2024, Bryan Murphy
#
'''
    Notes on database
    For remote access to the postgresql datbase you need to modify files and firewall
    
    In postgresql.conf
    #listen_addresses = 'localhost'
    listen_addresses = '*'

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


import psycopg2
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
# The database must already be setup and allowing connections from you device
#
# dbname -> stockanalysis is working database
# dbname -> stockanaltest is testing database table working
#
#
stock_db_params = {
# Postgres database Root password - 'Pgres#ma$ter'
    'dbname': 'stockanaltest',
    'user': 'stockfrk',
    'password': 'Stk$freak#pass7',
    'host': '192.168.123.66',  # Change this if your database is hosted elsewhere
    'port': 5432,  # Default PostgreSQL port
}

# Basic open database and provide the connection for others
def opendatabase():
    try:
        db = psycopg2.connect(**stock_db_params)
    except psycopg2.Error as e:
        logging.error(e)
        return(False)
    logging.info("Looks like we are connected to server %s and database %s" % (stock_db_params['host'], stock_db_params['dbname']))
    return(db)

def get_table_col_names(dbconn, table_str):
    print(f"Columns for table {table_str}")
    col_names = []
    try:
        cur = dbconn.cursor()
        cur.execute("select * from " + table_str + " LIMIT 0")
        for desc in cur.description:
            col_names.append(desc[0])        
    except psycopg2.Error as e:
        logging.error(e)
    return col_names

def main() -> None:
    db = opendatabase()
    db.close()

if __name__ == '__main__':
    main()