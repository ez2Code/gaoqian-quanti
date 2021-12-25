def calc_percent(close, start):
    close = float(close)
    start = float(start)
    return (close - start) * 100 / start
