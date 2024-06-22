# StockAnalysis
Python stock technical analysis with a twist

This project is about the idea of using technical analysis to determine the best stock to buy today. The best stock can change each day.

There are two parts to this process.

The parameters for technical analysis (weighting, number of periods to check, etc) will show the best stock based on market conditions.
You don't know which parameter set will work best in the current market conditions without backtesting.
The calculations for the technical analysis are boiled down to one number for each day. This number is constant given a parameter set and historical stock data. THIS IS NOT a buy or sell decision.
Given a set of technical anlysis caluclations for a parameter set the logic and values used to make buy / sell decisions determine the profit or loss.
Things to consider for buying: Is the ex-dividend date in the next few days? This can impact perception and behavior around
momentum and stock purchases. What profit level to sell (up or down). How much to invest if you decide to buy... There is more.
Given a set of data and buy/sell logic perform a backtest over one to two years. Store the results and rank stocks based on scores, purchases, and sales. Which set gives the most profits?
Tools used in the project:
Python
Python libraries that are downloaded.
Postgresql database

Python library list (I probably missed something that needs to be installed):
<li>yfinance</li>
<li>argparse</li>
numpy
pandas
icecream
psycopg2
logging
