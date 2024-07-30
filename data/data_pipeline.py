import yfinance as yf
import sqlite3
import pandas as pd
from datetime import datetime

# List of tickers
tickers = [
    'AAPL', 'ACN', 'ADI', 'ADP', 'ADSK', 'ANSS', 'APH', 'BABA', 'BIDU', 'BR', 'CRM',
    'FFIV', 'FIS', 'FISV', 'GOOG', 'GPN', 'IBM', 'INTC', 'INTU', 'IPGP', 'IT', 'JKHY', 
    'KEYS', 'KLAC', 'LRCX', 'MA', 'MCHP', 'MSFT', 'MSI', 'NVDA', 'NXPI', 'PYPL', 'SNPS', 
    'TEL', 'TTWO', 'TXN', 'V', 'VRSN'
]

# Download data from yfinance
data = yf.download(tickers, start='2018-01-01', end=datetime.today().strftime('%Y-%m-%d'))

# Create SQLite database and connect to it
conn = sqlite3.connect('data/main.db')
cursor = conn.cursor()

# CREATE OR REPLACE IF EXISTS 
cursor.execute('DROP TABLE IF EXISTS stock_data')

# Create a single table for all tickers
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        Ticker TEXT,
        Date TEXT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Adj_Close REAL,
        Volume INTEGER
    )
''')

# Insert data into the table
for ticker in tickers:
    ticker_data = data['Adj Close'][ticker].dropna().reset_index()
    ticker_data.columns = ['Date', 'Adj_Close']
    ticker_data['Open'] = data['Open'][ticker].reindex(ticker_data['Date']).values
    ticker_data['High'] = data['High'][ticker].reindex(ticker_data['Date']).values
    ticker_data['Low'] = data['Low'][ticker].reindex(ticker_data['Date']).values
    ticker_data['Close'] = data['Close'][ticker].reindex(ticker_data['Date']).values
    ticker_data['Volume'] = data['Volume'][ticker].reindex(ticker_data['Date']).values
    ticker_data['Ticker'] = ticker  # Add ticker column
    
    # Convert 'Date' to string format
    ticker_data['Date'] = ticker_data['Date'].astype(str)
    
    # Insert rows into the table
    for row in ticker_data.itertuples(index=False):
        cursor.execute('''
            INSERT INTO stock_data (Ticker, Date, High, Low, Close, Adj_Close, Volume) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (row.Ticker, row.Date, row.High, row.Low, row.Close, row.Adj_Close, row.Volume))

# Commit and close the connection
conn.commit()
conn.close()
