# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 15:32:17 2020

@author: tranl
"""

import warnings
warnings.simplefilter("ignore")

import time, sys
import numpy as np
import pandas as pd
from datetime import datetime
from binancepy import MarketData

min_in_candle ={'1m':1, '3m':3, '5m':5, '10m':10, '15m':15, '30m': 30, \
                '1h':60, '4h':240, '6h':360, '12h':720, '1d':1440}     
start_time = time.time()

def klns_to_df(market_data, feats):
    '''
    Add columns' name to dataFrame received from market and select the columns in feats
    
    feats -> ['_o', '_h', '_l', '_c', '_v']
    '''
    fts = list(str(f) for f in feats)
    _df = pd.DataFrame(market_data, columns = ['_t', '_o', '_h', '_l', '_c', '_v', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    _df[['_o', '_h', '_l', '_c', '_v']] = _df[['_o', '_h', '_l', '_c', '_v']].astype(float)
    return _df[fts]

def candle_no_limit(mkData, interval, startTime, endTime):
    '''
    Return dataFrame of mkData from startTime to endTime
    '''
    min_in_candle ={'1m':1, '3m':3, '5m':5, '10m':10, '15m':15, '30m': 30, \
                    '1h':60, '4h':240, '6h':360, '12h':720, '1d':1440}
    
    if not isinstance(startTime, int): 
        startTime = int(pd.Timestamp(datetime.strptime(startTime, '%d %b %Y %H:%M:%S')).value/10**6)
    if not isinstance(endTime, int): 
        endTime = int(pd.Timestamp(datetime.strptime(endTime, '%d %b %Y %H:%M:%S')).value/10**6)    
    sub_period = int((endTime - startTime)/(min_in_candle[interval]*60*1000))
    sub_start = startTime
    kln_df = None
    while sub_period > 0:
        numklns = min(sub_period, 500)
        market_df = mkData.candles_data(interval=interval, startTime=sub_start, limit=numklns)
        sub_df = klns_to_df(market_df, ['_t', '_o', '_h', '_l', '_c', '_v'])
        if kln_df is None:
            kln_df = pd.DataFrame(sub_df)
        else:
            kln_df = kln_df.append(pd.DataFrame(sub_df), ignore_index=True)
        sub_period -= numklns
        sub_start += numklns*min_in_candle[interval]*60*1000
    return kln_df

def main(args):
    '''
    Main function to call candle_no_limit()
    '''
    filepath = ''
    slist = [ 'BTCUSDT', 'ETHUSDT', 'BCHUSDT']
    for symbol in slist:
        data = MarketData(exchange='perpetual', testnet=False, symbol=symbol)
        interval = '3m'
        t_start = '01 Jul 2020 00:00:00' 
        t_end = '03 Jul 2020 00:00:00'

        klns = candle_no_limit(data, interval=interval, startTime=t_start, endTime=t_end)
        klnfile = filepath + "%s-%s.csv" % (symbol, interval)
        klns.to_csv(klnfile)

    print('\n\nElapsed time = ' + str(time.time()-start_time))
    print("\n#################### \n")

if __name__ == '__main__':
    main(sys.argv[1:])
