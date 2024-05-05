#!/bin/python

import yfinance as yf
import pandas as pd
import csv
import mysql.connector
import datetime

# Tickers' Lists ---------------------------------------------------------------------------------------

# S&P 500 tickers
sp500_tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()
for i in range(len(sp500_tickers)):
    if "." not in sp500_tickers[i]:
        continue
    else:
        sp500_tickers[i] = sp500_tickers[i].replace(".", "-")

# S&P 400 tickers
sp400_tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies')[0]['Symbol'].tolist()
for i in range(len(sp400_tickers)):
    if "." not in sp400_tickers[i]:
        continue
    else:
        sp400_tickers[i] = sp400_tickers[i].replace(".", "-")

# S&P 600 tickers
sp600_tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies')[0]['Symbol'].tolist()
for i in range(len(sp600_tickers)):
    if "." not in sp600_tickers[i]:
        continue
    else:
        sp600_tickers[i] = sp600_tickers[i].replace(".", "-")

        

magnificent_seven = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]
bitcoin = ["GBTC","IBIT", "FBTC", "ARKB", "BITB", "BTCO", "HODL", "BRRR", "MARA", "COIN", "MSTR"]
meme_stocks = ["DJT","AISP","NKLA","RDDT", "CVNA"]
ai_stocks = ["NVDA", "ARM", "AMD", "META", "GOOGL", "MSFT", "SMCI", "CRM", "ADBE", "GOAI", "AIAI.L", "WTAI"]
obesity_drugs = ["NVO", "LLY"]

big_list = list(set(sp500_tickers + sp400_tickers + sp600_tickers + magnificent_seven + bitcoin + meme_stocks + ai_stocks + obesity_drugs))
sp_1500 = list(sp500_tickers + sp400_tickers + sp600_tickers)

# Getting the data needed for table stockData from yfinance into a list of dictionaries ----------------------------------------------------

stock_data = []
list_of_fields = ['symbol', 'shortName', 'country', 'industry', 'sector', 'previousClose', 'beta', 'trailingPE', 'forwardPE', 'volume', 'averageVolume', 'averageVolume10days', 'marketCap', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyDayAverage', 'twoHundredDayAverage', 'bookValue', '52WeekChange', 'priceToBook']
#This sublist is for checking if fields that should be numerical are numerical and not strings
sublist_of_fields = ['previousClose', 'beta', 'trailingPE', 'forwardPE', 'volume', 'averageVolume', 'averageVolume10days', 'marketCap', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyDayAverage', 'twoHundredDayAverage', 'bookValue', '52WeekChange', 'priceToBook']
for ticker in big_list:
    temporary_dictionary = {}
    for k, v in yf.Ticker(ticker).info.items():
        if k in list_of_fields:
            if k in sublist_of_fields:
                if type(v) == str:
                    temporary_dictionary[k] = None
                else:
                    temporary_dictionary[k] = v
            else:
                temporary_dictionary[k] = v
    for item in list_of_fields:
        if item not in temporary_dictionary.keys():
            temporary_dictionary[item] = None
    percent_difference_from_52_week_low = temporary_dictionary['previousClose'] / temporary_dictionary['fiftyTwoWeekLow'] - 1
    percent_difference_from_52_week_high = temporary_dictionary['previousClose'] / temporary_dictionary['fiftyTwoWeekHigh'] - 1
    temporary_dictionary['percentDifferenceFrom52WeekLow'] = percent_difference_from_52_week_low
    temporary_dictionary['percentDifferenceFrom52WeekHigh'] = percent_difference_from_52_week_high
    if ticker in sp500_tickers:
        temporary_dictionary['spGroup'] = '500'
    elif ticker in sp400_tickers:
        temporary_dictionary['spGroup'] = '400'
    elif ticker in sp600_tickers:
        temporary_dictionary['spGroup'] = '600'
    else:
        temporary_dictionary['spGroup'] = 'Other'
    stock_data.append(temporary_dictionary)
    print(len(stock_data))

#Creating table stockData-----------------------------------------------------------------------------------------------

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="mydatabase"
)

mycursor = mydb.cursor()
mycursor.execute("SET FOREIGN_KEY_CHECKS = 0")
mycursor.execute("DROP TABLE IF EXISTS stockData")
mycursor.execute("SET FOREIGN_KEY_CHECKS = 1")
mycursor.execute("CREATE TABLE stockData (symbol VARCHAR(255) PRIMARY KEY, shortName VARCHAR(255), country VARCHAR(255), industry VARCHAR(255), sector VARCHAR(255), previousClose DECIMAL(10,4), beta DECIMAL(10,4), trailingPE DECIMAL(10,4), forwardPE DECIMAL(10,4), volume BIGINT, averageVolume BIGINT, averageVolume10days BIGINT, marketCap BIGINT, fiftyTwoWeekLow DECIMAL(10,4), fiftyTwoWeekHigh DECIMAL(10,4), fiftyDayAverage DECIMAL(10,4), twoHundredDayAverage DECIMAL(10,4), bookValue DECIMAL(10,4), priceToBook DECIMAL(10,4), 52WeekChange DECIMAL(10,4), percentDifferenceFrom52WeekLow DECIMAL(10,4), percentDifferenceFrom52WeekHigh DECIMAL(10,4), spGroup VARCHAR(255))")

for dictionary in stock_data:
    sql = "INSERT INTO stockData (symbol, shortName, country, industry, sector, previousClose, beta, trailingPE, forwardPE, volume, averageVolume, averageVolume10days, marketCap, fiftyTwoWeekLow, fiftyTwoWeekHigh, fiftyDayAverage, twoHundredDayAverage, bookValue, priceToBook, 52WeekChange, percentDifferenceFrom52WeekLow, percentDifferenceFrom52WeekHigh, spGroup) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (
        dictionary['symbol'],
        dictionary['shortName'],
        dictionary['country'],
        dictionary['industry'],
        dictionary['sector'],
        dictionary['previousClose'],
        dictionary['beta'],
        dictionary['trailingPE'],
        dictionary['forwardPE'],
        dictionary['volume'],
        dictionary['averageVolume'],
        dictionary['averageVolume10days'],
        dictionary['marketCap'],
        dictionary['fiftyTwoWeekLow'],
        dictionary['fiftyTwoWeekHigh'],
        dictionary['fiftyDayAverage'],
        dictionary['twoHundredDayAverage'],
        dictionary['bookValue'],
        dictionary['priceToBook'],
        dictionary['52WeekChange'],
        dictionary['percentDifferenceFrom52WeekLow'],
        dictionary['percentDifferenceFrom52WeekHigh'],
        dictionary['spGroup']
    )
    mycursor.execute(sql, val)
    mydb.commit()

mydb.commit()
#Creating CSV file stockData (csv files may be needed for simpler visualization tools)----------------------------------
with open('stockData.csv', mode='w', newline='') as stockDataFile:
    writer = csv.DictWriter(stockDataFile, fieldnames=stock_data[0].keys(), delimiter=';')
    writer.writeheader()
    for data in stock_data:
        writer.writerow(data)

#Creating table stockPrices--------------------------------------------------------------------------------------------

mycursor.execute("DROP TABLE IF EXISTS stockPrices")
mycursor.execute("CREATE TABLE stockPrices (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, symbol VARCHAR(255), open DECIMAL(10,4), high DECIMAL(10,4), low DECIMAL(10,4), close DECIMAL(10,4), adjustedClose DECIMAL(10,4), volume BIGINT, FOREIGN KEY (symbol) REFERENCES stockData(symbol) ON DELETE CASCADE)")

today = datetime.date.today()
start = today - datetime.timedelta(days=365)
end = today.strftime('%Y-%m-%d')

fieldnames = ['date', 'symbol', 'open', 'high', 'low', 'close', 'adjustedClose', 'volume']
with open('stockPrices.csv', mode='w', newline='') as csvfile:
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
    csv_writer.writeheader()


    for company in big_list:
        data = yf.download(company, start=start, end=end)
        for index, row in data.iterrows():
            insert_query = "INSERT INTO stockPrices (date, symbol, open, high, low, close, adjustedClose, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            insert_values = (index.strftime('%Y-%m-%d'), company, float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), float(row['Adj Close']), int(row['Volume']))
            mycursor.execute(insert_query, insert_values)

            csv_writer.writerow({
                'date': index.strftime('%Y-%m-%d'),
                'symbol': company,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'adjustedClose': float(row['Adj Close']),
                'volume': int(row['Volume'])
            })
        mydb.commit()
