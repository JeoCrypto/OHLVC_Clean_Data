#Cryptowolf.ai

import pandas as pd
import ccxt
import datetime

exchange = ccxt.binance() # Change the Exchange name to get the data from other Exchanges.
print(exchange.iso8601(exchange.milliseconds()), 'Loading markets')
markets = exchange.load_markets()
print(exchange.iso8601(exchange.milliseconds()), 'Markets loaded')

from_timestamp = exchange.parse8601('2022-01-01T00:00:00Z')

def get_data_from_exchange(coin, timestamp, filename):
df = pd.DataFrame()
from_timestamp = timestamp
now = exchange.milliseconds()
timeframe = '1h'
while from_timestamp < now:
print('Fetching data starting from: ', exchange.iso8601(from_timestamp))
ohlcvs = exchange.fetch_ohlcv(coin, timeframe, from_timestamp)
if not len(ohlcvs):
break
from_timestamp = ohlcvs[-1][0] + exchange.parse_timeframe(timeframe) * 1000
print(exchange.iso8601(exchange.milliseconds()), ohlcvs)
df = df.append(ohlcvs, ignore_index=True)
#df.to_csv(filename) # Optional
return df

def clean_data(df,filename):
data = pd.DataFrame(df)
df = pd.DataFrame(data)
df.columns = (["Time", "Open", "High", "Low", "Close", "Volume"])

def parse_dates(ts):
    return datetime.datetime.fromtimestamp(ts / 1000.0)

df["Time"] = df["Time"].apply(parse_dates)
# set index to Date Time
df.set_index("Time", inplace=True)
df['Close'] = df['Close'].astype(float)
df['Open'] = df['Open'].astype(float)
df['High'] = df['High'].astype(float)
df['Low'] = df['Low'].astype(float)
df['Volume'] = df['Volume'].astype(float)
df.dropna()
df.to_csv(filename)
print("Data exported to " + filename)
return df

'''

data_cleaned = get_data_from_exchange('BTC/USDT', from_timestamp, 'BTC_USDT_Clean.csv')

print(data_cleaned.head())


'''
