# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 15:32:17 2020

@author: tranl
"""

import warnings
warnings.simplefilter("ignore")

import time
import numpy as np
import pandas as pd
from datetime import datetime

from tqdm import tqdm
from binance.client import Client
from ultilities import barstr, timestr

start_time = time.time()

def tick_bars(df, m):
    t_df = pd.DataFrame(columns=['_t', '_o','_h', '_l', '_c', '_v'])
    low_, high_, open_ = 1e6, 0, df['_p'].iloc[0]
    t_ = df['_t'].iloc[0]
    ts, vol_ = 0, 0
    for i, x in enumerate(tqdm(df['_p'])):
        ts += df['_v'].iloc[i]*df['_p'].iloc[i]
        vol_ += df['_v'].iloc[i]
        if df['_p'].iloc[i] < low_: low_ = df['_p'].iloc[i]
        if df['_p'].iloc[i] > high_: high_ = df['_p'].iloc[i]
        if ts >= m:
            t_df = t_df.append({'_t': t_,\
                                '_o': open_, \
                                '_h': high_, \
                                '_l': low_, \
                                '_c': df['_p'].iloc[i], \
                                '_v': vol_ }, \
                                ignore_index=True)
            t_ = df['_t'].iloc[i]
            [low_, high_, open_] = [df['_p'].iloc[i]]*3
            ts,  vol_ = 0, 0
            continue
#    t_df = t_df.set_index('_t')
    return t_df

def get_ticks(symbol, start_time, filepath):    
    if not isinstance(start_time, int): start_time = int(pd.Timestamp(datetime.strptime(start_time, '%d %b %Y %H:%M:%S')).value/10**6)
    print("\nProcessing aggTrades for  %s from %s \n" % (symbol, timestr(start_time, end='s')))
    agg_trades = client.aggregate_trade_iter(symbol=symbol, start_str=start_time)
    trade_df = pd.DataFrame(list(agg_trades)).drop(['a', 'f','l','m','M'], axis=1)
    trade_df = trade_df.rename(columns={"p": "_p", "q": "_v", "T": "_t"})
    trade_df['_p'] = trade_df['_p'].astype(float)
    trade_df['_v'] = trade_df['_v'].astype(float)
    rawfile = filepath + "aggtrades-%s.csv" % (symbol)
    trade_df.to_csv(rawfile)
    return trade_df

def get_klines(symbol, kl_size, start_time, end_time, filepath):
    if isinstance(start_time, int): start_time = pd.to_datetime(start_time, unit='ms').strftime('%d %b %Y %H:%M:%S')
    if isinstance(end_time, int): end_time = timestr(end_time, end='s')
    print("\nProcessing data sub {} for {} from {} to {}\n".format(kl_size, symbol, start_time, end_time))
    klns = client.get_historical_klines(symbol, kl_size, start_time, end_time)
    kln_df = pd.DataFrame(klns, columns = ['_t', '_o', '_h', '_l', '_c', '_v','close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    kln_df = kln_df.drop(['close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'], axis=1) 
    klnfile = filepath + "-%s-%s.csv" % (symbol, kl_size)
    kln_df.to_csv(klnfile)
    return kln_df

### API
binance_api_key = ''    #Enter your own API-key here
binance_api_secret = '' #Enter your own API-secret here
client = Client(api_key=binance_api_key, api_secret=binance_api_secret)

filepath_tik = '' #'data/ticks/' 
filepath_kln = '' #'data/klines/'
slist = [ 'BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'LINKUSDT', 'LTCUSDT', 'XTZUSDT' ]  #'BNBUSDT', 'LINKUSDT', 'ETCUSDT', 'XTZUSDT', 'TRXUSDT', 'XRPUSDT', 'EOSUSDT'
start_time = time.time()
kl_size = '1m'
# t_start, t_end = '10 May 2020 00:00:00', '11 May 2020 00:00:00'
period = 6*60
t_end = int(client.get_server_time()['serverTime'])
t_start =  t_end - period*60*1000
for symbol in slist:
   df = get_klines(symbol, kl_size, t_start, t_end, filepath_kln)
   t_df = get_ticks(symbol, t_start, filepath_tik)
   dollar_bin = (t_df['_p'] @ t_df['_v'])/(1000*(24*60)/5)
   dollar_bin = int(dollar_bin)*1000
   bar_df = tick_bars(t_df, dollar_bin)
   bar_df.to_csv(filepath_tik + "tick-%s.csv" % (symbol))

print('\n' + barstr(text='Elapsed time = {} seconds'.format(round(time.time()-start_time,2))))
print(barstr(text="", space_size=0))

