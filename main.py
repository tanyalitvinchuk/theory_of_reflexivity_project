#!/bin/python

import yfinance as yf
import pandas as pd
import csv
import mysql.connector
import datetime


# Defining class Tickers that is used to return lists of tickers-----------------------------------------------------

class Tickers:
    def __init__(self):
        self.sp500url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        self.sp400url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        self.sp600url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        self.magnificent_seven = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]
        self.bitcoin = ["GBTC", "IBIT", "FBTC", "ARKB", "BITB", "BTCO", "HODL", "BRRR", "MARA", "COIN", "MSTR"]
        self.meme_stocks = ["DJT", "AISP", "NKLA", "RDDT", "CVNA"]
        self.ai_stocks = ["NVDA", "ARM", "AMD", "META", "GOOGL", "MSFT", "SMCI", "CRM", "ADBE", "GOAI", "WTAI"]
        self.obesity_drugs = ["NVO", "LLY"]
        self.other_tickers = ['TMST','AMEH','TSLA','AAPL'] # For checking some tickers
        self.tickers_cache = {}

    def __str__(self):
        return "Available tickers' lists are: 'sp500_tickers', 'sp400_tickers', 'sp600_tickers', 'sp_1500', " \
               "'magnificent_seven', 'bitcoin', 'meme_stocks','ai_stocks','obesity_drugs', 'big_list', 'other_tickers'"

    def fetch_tickers(self, url):
        if url not in self.tickers_cache:
            data = pd.read_html(url)[0]['Symbol'].tolist()
            self.tickers_cache[url] = [ticker.replace(".", "-") for ticker in data]
        return self.tickers_cache[url]

    def get_tickers_list(self, type: str):
        if type == 'sp500_tickers':
            return self.fetch_tickers(self.sp500url)
        elif type == 'sp400_tickers':
            return self.fetch_tickers(self.sp400url)
        elif type == 'sp600_tickers':
            return self.fetch_tickers(self.sp600url)
        elif type == 'sp_1500':
            return self.get_tickers_list('sp500_tickers') + self.get_tickers_list('sp400_tickers') + self.get_tickers_list('sp600_tickers')
        elif type == 'big_list':
            return list(set(self.get_tickers_list('sp_1500') + self.magnificent_seven + self.bitcoin + self.meme_stocks + self.ai_stocks + self.obesity_drugs))
        else:
            return getattr(self, type, [])

# Defining class StockData that is used to get data from yfinance.info into the list of dictionaries for the first table------------------------------------------

class GetStockData:
    def __init__(self, type: str):
        self.tickers_list = Tickers().get_tickers_list(type)
        self.sp500_tickers = Tickers().get_tickers_list("sp500_tickers")
        self.sp400_tickers = Tickers().get_tickers_list("sp400_tickers")
        self.sp600_tickers = Tickers().get_tickers_list("sp600_tickers")
        self.stock_data = {}
        self.available_tickers = []
        self.unavailable_tickers = []
        self.list_of_fields = ['symbol', 'shortName', 'country', 'industry', 'sector', 'previousClose', 'beta',
                               'trailingPE', 'forwardPE', 'volume', 'averageVolume', 'averageVolume10days', 'marketCap',
                               'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyDayAverage', 'twoHundredDayAverage',
                               'bookValue', '52WeekChange', 'priceToBook']
        # This sublist is for checking if fields that should be numerical are numerical and not strings
        self.sublist_of_fields = ['previousClose', 'beta', 'trailingPE', 'forwardPE', 'volume', 'averageVolume',
                                  'averageVolume10days', 'marketCap', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
                                  'fiftyDayAverage', 'twoHundredDayAverage', 'bookValue', '52WeekChange', 'priceToBook']
        self.getting_the_data()

    def getting_the_data(self):
        for ticker in self.tickers_list:
            temporary_dictionary = {}
            for k, v in yf.Ticker(ticker).info.items():
                if k in self.list_of_fields:
                    if k in self.sublist_of_fields:
                        if type(v) == str:
                            temporary_dictionary[k] = None
                        else:
                            temporary_dictionary[k] = v
                    else:
                        temporary_dictionary[k] = v

            for item in self.list_of_fields:
                if item not in temporary_dictionary.keys():
                    temporary_dictionary[item] = None

            previous_close = temporary_dictionary['previousClose']
            fifty_two_week_low = temporary_dictionary['fiftyTwoWeekLow']
            fifty_two_week_high = temporary_dictionary['fiftyTwoWeekHigh']

            percent_difference_from_52_week_low = None
            percent_difference_from_52_week_high = None

            if previous_close is not None and fifty_two_week_low is not None:
                percent_difference_from_52_week_low = (previous_close / fifty_two_week_low) - 1

            if previous_close is not None and fifty_two_week_high is not None:
                percent_difference_from_52_week_high = (previous_close / fifty_two_week_high) - 1

            temporary_dictionary['percentDifferenceFrom52WeekLow'] = percent_difference_from_52_week_low
            temporary_dictionary['percentDifferenceFrom52WeekHigh'] = percent_difference_from_52_week_high

            if ticker in self.sp500_tickers:
                temporary_dictionary['spGroup'] = '500'
            elif ticker in self.sp400_tickers:
                temporary_dictionary['spGroup'] = '400'
            elif ticker in self.sp600_tickers:
                temporary_dictionary['spGroup'] = '600'
            else:
                temporary_dictionary['spGroup'] = 'Other'
            if temporary_dictionary['symbol'] is None:
                self.unavailable_tickers.append(ticker)
            else:
                self.stock_data[ticker] = temporary_dictionary
                self.available_tickers.append(ticker)
                print(len(self.stock_data))
        print(f"Unavailable tickers: {self.unavailable_tickers}")


# Defining class DatabaseTables that is used to write the data to two database tables and also to csv files-------------

class DatabaseTables:
    def __init__(self,type):
        self.stock_data = GetStockData(type).stock_data
        print(self.stock_data)

        self.mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="ukrainewillwin",
            database="mydatabase"
        )
        self.mycursor = self.mydb.cursor()

        self.create_table_stockdata()
        self.create_csv_stockdata()
        self.create_table_stockprices()



    def create_table_stockdata(self):
        self.mycursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.mycursor.execute("DROP TABLE IF EXISTS stockData")
        self.mycursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.mycursor.execute(
            "CREATE TABLE stockData (symbol VARCHAR(255) PRIMARY KEY, shortName VARCHAR(255), country VARCHAR(255), industry VARCHAR(255), sector VARCHAR(255), previousClose DECIMAL(10,4), beta DECIMAL(10,4), trailingPE DECIMAL(10,4), forwardPE DECIMAL(10,4), volume BIGINT, averageVolume BIGINT, averageVolume10days BIGINT, marketCap BIGINT, fiftyTwoWeekLow DECIMAL(10,4), fiftyTwoWeekHigh DECIMAL(10,4), fiftyDayAverage DECIMAL(10,4), twoHundredDayAverage DECIMAL(10,4), bookValue DECIMAL(10,4), priceToBook DECIMAL(10,4), 52WeekChange DECIMAL(10,4), percentDifferenceFrom52WeekLow DECIMAL(10,4), percentDifferenceFrom52WeekHigh DECIMAL(10,4), spGroup VARCHAR(255))")

        for dictionary in self.stock_data.values():
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
            self.mycursor.execute(sql, val)
            self.mydb.commit()

    def create_csv_stockdata(self):
        with open('stockData.csv', mode='w', newline='') as stockDataFile:
            writer = csv.DictWriter(stockDataFile, fieldnames=list(self.stock_data.values())[0].keys(), delimiter=';')
            writer.writeheader()
            for data in self.stock_data.values():
                writer.writerow(data)
    def create_table_stockprices(self):
        self.mycursor.execute("DROP TABLE IF EXISTS stockPrices")
        self.mycursor.execute(
            "CREATE TABLE stockPrices (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, symbol VARCHAR(255), open DECIMAL(10,4), high DECIMAL(10,4), low DECIMAL(10,4), close DECIMAL(10,4), adjustedClose DECIMAL(10,4), volume BIGINT, 52WeekLow DECIMAL (10,4), 52WeekHigh DECIMAL (10,4))")

        today = datetime.date.today()
        start = today - datetime.timedelta(days=365)
        end = today.strftime('%Y-%m-%d')

        fieldnames = ['date', 'symbol', 'open', 'high', 'low', 'close', 'adjustedClose', 'volume', '52WeekLow', '52WeekHigh']
        tickers_list = Tickers().get_tickers_list(tickers)
        with open('stockPrices.csv', mode='w', newline='') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            csv_writer.writeheader()

            for company in tickers_list:
                data = yf.download(company, start=start, end=end)

                for index, row in data.iterrows():
                    insert_query = "INSERT INTO stockPrices (date, symbol, open, high, low, close, adjustedClose, volume, fiftyTwoWeekLow, fiftyTwoWeekHigh) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    insert_values = (
                        index.strftime('%Y-%m-%d'), company, float(row['Open']), float(row['High']), float(row['Low']),
                        float(row['Close']), float(row['Adj Close']), int(row['Volume']), float(self.stock_data[company]['fiftyTwoWeekLow']), float(self.stock_data[company]['fiftyTwoWeekHigh']))
                    self.mycursor.execute(insert_query, insert_values)

                    csv_writer.writerow({
                        'date': index.strftime('%Y-%m-%d'),
                        'symbol': company,
                        'open': float(row['Open']),
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'close': float(row['Close']),
                        'adjustedClose': float(row['Adj Close']),
                        'volume': int(row['Volume']),
                        '52WeekLow': float(self.stock_data[company]['fiftyTwoWeekLow']),
                        '52WeekHigh': float(self.stock_data[company]['fiftyTwoWeekHigh'])
                    })
                self.mydb.commit()


dictionary_for_choosing_tickers = {1:'sp500_tickers', 2:'sp400_tickers', 3:'sp600_tickers', 4:'sp_1500', 5:'magnificent_seven',
                               6:'bitcoin', 7:'meme_stocks', 8:'ai_stocks', 9:'obesity_drugs', 10:'big_list',
                               11:'other_tickers'}

print("Please, enter a number corresponding to the tickers list you want to use. \nAvailable options are:")
for k, v in dictionary_for_choosing_tickers.items():
    print(f" {k} - {v};")
tickers = dictionary_for_choosing_tickers[int(input("Your choice: "))]

a = DatabaseTables(tickers)
