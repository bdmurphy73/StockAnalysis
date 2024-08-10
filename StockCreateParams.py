#Python 3.10
#
#
#  Copyright (c) 2024, Bryan Murphy
#
#  license: GNU LGPL
#

#######################################################################
# This is used to create the parameters table used for TA calculations and weights. Each variable will have a range and calculate 10 steps in that range.
# The table will be created by iterating across each parameter one at a time.
# The results will be stored in the parameters table as a full set. THe ID of that parameter row will be used later as the reference.

#### THIS PROGRAM IS SETUP TO WIPE THE PARAMS TABLE AND START OVER UNLESS THIS FLAG IS SET TO FALSE

'''
The number of steps each parameter takes will create a seperate entry with all the other permutations. 
With 21 parameters and 10 steps for each parameter, the table would have 10^23 entries - TOO MUCH
5 steps per perameter is 5^23 = 11,920,928,955,078,125 entries
I am starting with 3 steps per parameter. 3^23 = 94,143,178,827 entries. 94 Billion entries in the table
After the system has processed (used the parameteres) too see which entries provide the 
best results I will focus on that area with more steps for the parameters.

The variable globalstep = 3 is used below.

'''

from icecream import ic # help with debug
#from operator import itemgetter #????

import time     # to calculate the run time

from StockCommon import * # This module loads psycopg2 (for database access) and logging
from StockPgresDB import * # Has the table schemas and initialization for database.

#
# x = [i for i in range(10) if i % 2 == 0]
#
#
# Create a recursive function.
# Inputs are dictionary of param table. 
# Inputs name of value to change, limits and steps
# check if this name is the last in the list.
# yes iterate 
# No recursive call with next iterration

paramlist = ['sma_fast', 'sma_fast_mul', 'sma_slow', 'sma_slow_mul', 'ema_fast', 'ema_fast_mul',
        'ema_slow', 'ema_slow_mul', 'momentum_n', 'momentum_mul', 'macd_fast', 'macd_slow', 'macd_mul', 
        'macd_signal', 'macd_sig_mul', 'bb_n', 'bb_sigma', 'bb_mul', 'rsi_n', 'rsi_mul',
        'stoch_n', 'stoch_mul', 'ttmsq_mul']
addedlist = [ 'profit_target', 'loss_target', 'time_limit', 'roc_mul', 'roc_n']

# TTM squese ??? tripple squese

Wipetable = True
globalstep = 3
totalcnt = 0


paramvariation = {}
paramvariation[paramlist[0]] = []
paramvariation[paramlist[0]].append("sma_fast") # parameter name
paramvariation[paramlist[0]].append(5) # parameter lower limit
paramvariation[paramlist[0]].append(12) # parameter upper limit
paramvariation[paramlist[0]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[0]].append(9) # The current value while in the recursion
paramvariation[paramlist[1]] = []
paramvariation[paramlist[1]].append("sma_fast_mul")
paramvariation[paramlist[1]].append(0.5)
paramvariation[paramlist[1]].append(2.0)
paramvariation[paramlist[1]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[1]].append(1) # The current value while in the recursion
paramvariation[paramlist[2]] = []
paramvariation[paramlist[2]].append("sma_slow")
paramvariation[paramlist[2]].append(12)
paramvariation[paramlist[2]].append(20)
paramvariation[paramlist[2]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[2]].append(12) # The current value while in the recursion
paramvariation[paramlist[3]] = []
paramvariation[paramlist[3]].append("sma_slow_mul")
paramvariation[paramlist[3]].append(0.5)
paramvariation[paramlist[3]].append(2.0)
paramvariation[paramlist[3]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[3]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[4]] = []
paramvariation[paramlist[4]].append("ema_fast")
paramvariation[paramlist[4]].append(5)
paramvariation[paramlist[4]].append(12)
paramvariation[paramlist[4]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[4]].append(5) # The current value while in the recursion
paramvariation[paramlist[5]] = []
paramvariation[paramlist[5]].append("ema_fast_mul")
paramvariation[paramlist[5]].append(0.5)
paramvariation[paramlist[5]].append(2.0)
paramvariation[paramlist[5]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[5]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[6]] = []
paramvariation[paramlist[6]].append("ema_slow")
paramvariation[paramlist[6]].append(12)
paramvariation[paramlist[6]].append(20)
paramvariation[paramlist[6]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[6]].append(12) # The current value while in the recursion
paramvariation[paramlist[7]] = []
paramvariation[paramlist[7]].append("ema_slow_mul")
paramvariation[paramlist[7]].append(0.5)
paramvariation[paramlist[7]].append(2.0)
paramvariation[paramlist[7]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[7]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[8]] = []
paramvariation[paramlist[8]].append("momentum_n")
paramvariation[paramlist[8]].append(5)
paramvariation[paramlist[8]].append(12)
paramvariation[paramlist[8]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[8]].append(5) # The current value while in the recursion
paramvariation[paramlist[9]] = []
paramvariation[paramlist[9]].append("momentum_mul")
paramvariation[paramlist[9]].append(0.5)
paramvariation[paramlist[9]].append(2.0)
paramvariation[paramlist[9]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[9]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[10]] = []
paramvariation[paramlist[10]].append("macd_fast")
paramvariation[paramlist[10]].append(9)
paramvariation[paramlist[10]].append(16)
paramvariation[paramlist[10]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[10]].append(9) # The current value while in the recursion
paramvariation[paramlist[11]] = []
paramvariation[paramlist[11]].append("macd_slow")
paramvariation[paramlist[11]].append(22)
paramvariation[paramlist[11]].append(32)
paramvariation[paramlist[11]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[11]].append(22) # The current value while in the recursion
paramvariation[paramlist[12]] = []
paramvariation[paramlist[12]].append('macd_mul')
paramvariation[paramlist[12]].append(0.5)
paramvariation[paramlist[12]].append(2.0)
paramvariation[paramlist[12]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[12]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[13]] = []
paramvariation[paramlist[13]].append('macd_signal')
paramvariation[paramlist[13]].append(5)
paramvariation[paramlist[13]].append(10)
paramvariation[paramlist[13]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[13]].append(5) # The current value while in the recursion
paramvariation[paramlist[14]] = []
paramvariation[paramlist[14]].append('macd_sig_mul')
paramvariation[paramlist[14]].append(0.5)
paramvariation[paramlist[14]].append(2.0)
paramvariation[paramlist[14]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[14]].append(0.5) # The current value while in the recursion
paramvariation[paramlist[15]] = []
paramvariation[paramlist[15]].append('bb_n')
paramvariation[paramlist[15]].append(22)
paramvariation[paramlist[15]].append(30)
paramvariation[paramlist[15]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[15]].append(22) # The current value while in the recursion
paramvariation[paramlist[16]] = []
paramvariation[paramlist[16]].append('bb_sigma')
paramvariation[paramlist[16]].append(22)
paramvariation[paramlist[16]].append(30)
paramvariation[paramlist[16]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[16]].append(22) # The current value while in the recursion
paramvariation[paramlist[17]] = []
paramvariation[paramlist[17]].append('bb_mul')
paramvariation[paramlist[17]].append(22)
paramvariation[paramlist[17]].append(30)
paramvariation[paramlist[17]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[17]].append(22) # The current value while in the recursion
paramvariation[paramlist[18]] = []
paramvariation[paramlist[18]].append('rsi_n')
paramvariation[paramlist[18]].append(22)
paramvariation[paramlist[18]].append(30)
paramvariation[paramlist[18]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[18]].append(22) # The current value while in the recursion
paramvariation[paramlist[19]] = []
paramvariation[paramlist[19]].append('rsi_mul')
paramvariation[paramlist[19]].append(22)
paramvariation[paramlist[19]].append(30)
paramvariation[paramlist[19]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[19]].append(22) # The current value while in the recursion
paramvariation[paramlist[20]] = []
paramvariation[paramlist[20]].append('stoch_n')
paramvariation[paramlist[20]].append(22)
paramvariation[paramlist[20]].append(30)
paramvariation[paramlist[20]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[20]].append(22) # The current value while in the recursion
paramvariation[paramlist[21]] = []
paramvariation[paramlist[21]].append('stoch_mul')
paramvariation[paramlist[21]].append(22)
paramvariation[paramlist[21]].append(30)
paramvariation[paramlist[21]].append(1) # Is the step integer or real - impacts rounding
paramvariation[paramlist[21]].append(22) # The current value while in the recursion
paramvariation[paramlist[22]] = []
paramvariation[paramlist[22]].append('ttmsq_mul')
paramvariation[paramlist[22]].append(1.0)
paramvariation[paramlist[22]].append(10.0)
paramvariation[paramlist[22]].append(0) # Is the step integer or real - impacts rounding
paramvariation[paramlist[22]].append(1.0) # The current value while in the recursion


def storeparm(dict, db):
    # at the end of recursion cycle. Store the full dict in database
    # This will store the value of each key in the column labeled by the key.
    global totalcnt
    dbcur=db.cursor()
    lastparamkey = paramlist[len(paramlist)-1]
    # build sql from dictionary
    sql = f"INSERT INTO {stock_db_tables['Params']} ("
    for k in dict.keys():
        if k == lastparamkey:
            sql = sql + k
        else:
            sql = sql + k + ", "
    sql = sql + ") VALUES ("
    for k, items in dict.items():
        if k == lastparamkey:
            sql = sql + "'" + str(items[4]) + "'"
        else:
            sql = sql + "'" + str(items[4]) + "', "
    sql = sql + ")"
    try:
        logging.debug(f"Sql = {sql}")
        dbcur.execute(sql)
        sql = sql # + ";"
    except psycopg2.Error as e:
        logging.error(e)
    
    #logging.debug(f"Sql = {sql}")
    #logging.debug(ic(curkey, lvalue))
    totalcnt = totalcnt + 1
    return

def paramiterate(paramdict, paramlist, curkey, db):
    # if last in list for loop calculate
    # else get the next in list and recursive call
    #logging.info("Starting paramiterate")
    global globalstep
    lastparamkey = paramlist[len(paramlist)-1]

    #logging.debug(f"curkey = {curkey}: Lastkey is {lastparamkey}")
    #logging.debug(ic(curkey, paramdict[curkey]))
    #logging.debug(f"paramdict[curkey] = {paramdict[curkey]}")
    stp =   (paramdict[curkey][2]-paramdict[curkey][1])/globalstep
    if paramdict[curkey][3]:
        stp = round(stp)
        #logging.debug("Rounding")
    else:
        #logging.debug("Not Rounding")
        t=0
    lvalue = paramdict[curkey][1]
    paramdict2 = paramdict.copy()
    #logging.debug(f"stp = {stp}")
    
    if curkey == lastparamkey:        # At the end, itterate and store
        #logging.debug(f"curkey = {lastparamkey}. Starting store loop")
        while (lvalue <= paramdict[curkey][2]): # high value for paramter
            #logging.debug(f"curkey = {curkey} and lvalue = {lvalue}")
            paramdict2[curkey][4] = lvalue # store the lvalue in current
            storeparm(paramdict, db)
            lvalue = lvalue + stp
    else:
        while (lvalue <= paramdict[curkey][2]): # high value for paramter
            #logging.debug(f"Recursion: curkey = {curkey} and lvalue = {lvalue}")
            tindex = paramlist.index(curkey)
            ncurkey = paramlist[tindex+1] # The next param in the list
            paramdict2[curkey][4] = lvalue # store the lvalue in current
            #logging.debug(f"New curkey is {ncurkey}")
            paramiterate(paramdict2, paramlist, ncurkey, db)
            lvalue = lvalue + stp
    return

 

def main() -> None:
    logging.info('Starting create params table')
    start_time = time.time()
    #for key, index in paramvariation.items():
    #    logging.info(ic(key,index))
    db = opendatabase()
    if db == False:
        logging.error("No Database. We can't continue")
        return
# To make sure we have a clean table we will drop the table and then recreate
# from the schema in StockPgresDB.py    
    worked = Initializedb(db, True, stock_db_tables['Params'], params_table) # In StockPgresDB.py
    if worked == False:
        logging.error("The table could not be created.")
        db.close()
        return
    
    paramiterate(paramvariation, paramlist, paramlist[0], db)
    db.commit()
    db.close()

    logging.info(f"Total store count is {totalcnt}")
    stop_time = time.time()
    ic((stop_time - start_time))
    return

if __name__ == '__main__':
    main()  

