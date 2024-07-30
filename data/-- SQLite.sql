-- SQLite
SELECT  df.Ticker,
        date(df.Date) as dt,
        round(df.Open,2) as Open,
        round(df.High, 2) as High,
        round(df.Low, 2) as Low,
        round(df.Close, 2) as Close,
        round(df.Adj_Close, 2) as Adj_Close,
        df.Volume
FROM stock_data AS df
ORDER BY df.Ticker ASC, dt DESC;