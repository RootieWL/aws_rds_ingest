import os
import pandas as pd
import numpy as np
import time
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.sectorperformance import SectorPerformances
from datetime import datetime

key = 'A8XVL8VQBC52RUTH'
ts = TimeSeries(key, output_format='pandas')

stock_symbol = ['AXP','TRV','HD','INTC','PFE']
index_symbol = ['SPX','NDAQ','DJIA'] 

price_col = {'1. open':'daily_price_open', '2. high': 'daily_price_high',
             '3. low' : 'daily_price_low', '4. close': 'daily_price_close',
             '5. volume': 'daily_price_volume'}

rank_col = {'Rank A: Real-Time Performance': 'rank_A',
            'Rank B: Day Performance': 'rank_B',
            'Rank C: Day Performance': 'rank_C',
            'Rank D: Month Performance': 'rank_D',
            'Rank E: Month Performance': 'rank_E',
            'Rank F: Year-to-Date (YTD) Performance': 'rank_F',
            'Rank G: Year Performance': 'rank_G',
            'Rank H: Year Performance': 'rank_H',
            'Rank I: Year Performance': 'rank_I',
            'Rank J: Year Performance': 'rank_J'}

index_hist = pd.DataFrame()

for index in index_symbol:
    data, meta_data = ts.get_daily(symbol=index, outputsize='full')
    data = data['2016-01-01':].reset_index()
    data['index_symbol'] = index
    index_hist = index_hist.append(data)

print('Job done for index_hist')

time.sleep(10)

stock_hist = pd.DataFrame()    
    
for stock in stock_symbol:
    data, meta_data = ts.get_daily(symbol=stock, outputsize='full')
    data = data['2016-01-01':].reset_index()
    data['stock_symbol'] = stock
    stock_hist = stock_hist.append(data)
    time.sleep(10)

print('Job done for stock_hist')

time.sleep(10)

sp = SectorPerformances(key=key,output_format='pandas')

sector_df, meta_data = sp.get_sector()
sector_df['sector_name'] = sector_df.index
sector_df['date'] = datetime.today().strftime('%Y-%m-%d')

print('Job done for sector_daily')

#Output pandas to csv file
index_hist = index_hist.reset_index(drop=True).rename(columns= price_col)
index_hist.to_csv('index_hist.csv')
print('index_hist.csv file generated')

stock_hist = stock_hist.reset_index(drop=True).rename(columns= price_col)
stock_hist.to_csv('stock_hist.csv')
print('stock_hist.csv file generated')

sector_df = sector_df.rename(columns=rank_col).reset_index(drop=True).fillna(0)
sector_df.to_csv('sector_daily.csv')
print('sector_daily.csv file generated')