import pandas as pd
import math
import sys
import time

ohlc = pd.read_csv('ohlc.csv')

dates = []
opens = []
highs = []
lows = []
closes = []
volumes = []

interval = None
try:
    interval = int(sys.argv[1])
except:
    print('You must pass your aggregation interval to the script, e.g: python main.py 5')
    exit()

row_count = ohlc.shape[0]
output_row_count = math.ceil(row_count / interval)
start_ts = time.time()
duration = 0

print('Aggregating 1m OHLC candles into', str(interval) + 'm intervals. Processing', row_count, 'input candles:')

for n in range(0, output_row_count):
    current_ts = time.time()
    duration = round(current_ts - start_ts, 1)
    start = n * 5
    end = (n * 5) + 5
    candle_subset = ohlc.iloc[start:end]
    date = candle_subset.iloc[0]['Local time']
    open = round(candle_subset.iloc[0]['Open'], 5)
    close = round(candle_subset.iloc[4]['Close'], 5)
    high = round(max(candle_subset['High'].values), 5)
    low = round(min(candle_subset['Low'].values), 5)
    volume = round(sum(candle_subset['Volume'].values))
    dates.append(date)
    opens.append(open)
    closes.append(close)
    highs.append(high)
    lows.append(low)
    volumes.append(volume)
    progress = (n / output_row_count) * 100
    if progress % 1 == 0:
        print(str(round(progress)) + '%')

output_ohlc = pd.DataFrame({
    'Date': dates,
    'Open': opens,
    'High': highs,
    'Low': lows,
    'Close': closes,
    'Volume': volumes
})

output_ohlc.to_csv('output_ohlc.csv')

print('Finished aggregating OHLC data in', str(duration), 'seconds!')
