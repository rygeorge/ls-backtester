import pandas as pd
import yfinance as yf
import numpy as np
from numba import jit, prange
from datetime import datetime
from datetime import datetime

tickers = ['AAPL', 'ACN', 'ADI', 'ADP', 'ADSK', 'ANSS', 'APH', 'BABA', 'BIDU', 'BR', 'CRM',
           'FFIV', 'FIS', 'FISV', 'GOOG', 'GPN', 'IBM', 'INTC', 'INTU', 'IPGP', 'IT', 'JKHY', 
           'KEYS', 'KLAC', 'LRCX', 'MA', 'MCHP', 'MSFT', 'MSI', 'NVDA', 'NXPI', 'PYPL', 'SNPS', 
           'TEL', 'TTWO', 'TXN', 'V', 'VRSN']

data = yf.download(tickers, '2018-01-01', datetime.today().strftime('%Y-%m-%d'))['Adj Close']

data.to_csv('~/Downloads/data.csv')
daily_stock_returns = (data - data.shift(1)) / data.shift(1)
daily_stock_returns.dropna(inplace=True)

# Function to rank the daily returns
@jit(nopython=True, parallel=True)
def rank_data(daily_returns):
    rows, cols = daily_returns.shape
    rank_matrix = np.empty((rows, cols))
    for i in prange(rows):
        rank_matrix[i] = np.argsort(np.argsort(-daily_returns[i])) + 1
    return rank_matrix

# Convert daily returns DataFrame to numpy array for performance
daily_returns_array = daily_stock_returns.values
rank_matrix = rank_data(daily_returns_array)

# Function to generate trade signals
@jit(nopython=True, parallel=True)
def generate_signals(rank_matrix, threshold):
    rows, cols = rank_matrix.shape
    signal_matrix = np.ones((rows, cols))
    for i in prange(rows):
        for j in prange(cols):
            if rank_matrix[i, j] < threshold:
                signal_matrix[i, j] = -1
    return signal_matrix

signal_matrix = generate_signals(rank_matrix, 22)

# Calculating returns based on trade signals
@jit(nopython=True, parallel=True)
def calculate_returns(signal_matrix, daily_returns_array):
    rows, cols = signal_matrix.shape
    returns_matrix = np.empty((rows, cols))
    for i in prange(rows - 1):  # Avoid the last row as we shift -1
        returns_matrix[i] = signal_matrix[i] * daily_returns_array[i + 1]
    returns_matrix[-1] = 0  # The last row is set to zero
    return returns_matrix

returns_matrix = calculate_returns(signal_matrix, daily_returns_array)
strategy_returns = np.sum(returns_matrix, axis=1) / len(tickers)

if strategy_returns.size > 0:
    # Cumulative Returns
    cumulative_returns = np.cumprod(strategy_returns + 1)

    # Sharpe Ratio
    # Assuming risk-free rate as 0 for simplicity
    daily_rf_rate = 0
    annual_rf_rate = daily_rf_rate * 252
    strategy_volatility = np.std(strategy_returns) * np.sqrt(252)
    sharpe_ratio = (np.mean(strategy_returns) - annual_rf_rate) / strategy_volatility

    # Drawdown
    cum_max = np.maximum.accumulate(cumulative_returns)
    drawdown = (cumulative_returns - cum_max) / cum_max
    max_drawdown = np.min(drawdown)

    # Print the results
    print("Cumulative Returns:")
    print(cumulative_returns[-1] if cumulative_returns.size > 0 else "No trades executed.")
    print("\nSharpe Ratio:")
    print(sharpe_ratio)
    print("\nMax Drawdown:")
    print(max_drawdown)
else:
    print("No trades executed. Cannot compute performance metrics.")
