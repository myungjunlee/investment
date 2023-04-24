import ccxt
import pandas as pd
import datetime
import sqlite3

def to_mstimestamp(str):
    str = datetime.datetime.strptime(str, "%Y-%m-%d %H:%M:%S")
    str = datetime.datetime.timestamp(str)
    str = int(str)*1000
    return str

binance = ccxt.binance()

con = sqlite3.connect("./day1.db")

def ohlcv_data():

    final_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    input_date = 0

    while True:
        if input_date == 0:
            start_date = to_mstimestamp('2017-01-01 00:00:00')
        else:
            start_date = to_mstimestamp(str(input_date + datetime.timedelta(days=1)))
        # end_date = to_mstimestamp('2017-12-31 23:59:59')
        ohlcv = binance.fetch_ohlcv('BTC/USDT', timeframe='1d', since=start_date, limit=1000)
        # ohlcv = binance_usdm.fetch_ohlcv('BTC/USDT', timeframe='1m', since=start_date, limit=1000, params={'endTime':end_date})

        df = pd.DataFrame(ohlcv, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Date'] = pd.to_datetime(df['Date'], unit='ms') + datetime.timedelta(hours=9)


        df.set_index('Date', inplace=False)
        df = df.astype({'Open':'float','High':'float','Low':'float','Close':'float','Volume':'float'}) # type을 float로 변경
        df = df.sort_index(ascending=True) #인덱스를 오름차순으로 정리
        print(df)
        
        if len(final_df) != 0 and len(df) == 0:
            break
        else:
            final_df = pd.concat([final_df,df],axis=0, ignore_index=True)
            input_date = df['Date'][len(df)-1]
            print(input_date)

    return final_df

trading_df=ohlcv_data()
trading_df.to_sql("bitcoin1d", con, index=False, if_exists='replace')
