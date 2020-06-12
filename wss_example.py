# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 00:16:07 2020

@author: tranl
"""
import time, os
import numpy as np
import pandas as pd
import websocket
import threading
import json

from binancepy import Client
from ultilities import timestr, barstr

### wss functions
def on_message(ws, message):
    mess = json.loads(message)
    if mess['e'] == 'depthUpdate':
        symbol = kln['s'].upper()
        bids, asks = mess['b'][:3], mess['a'][:3]
        BidAsk[symbol].append({'_t': int(mess['T']), '_b': bids, '_a': asks})            
    elif mess['e'] == 'aggTrade':
        symbol = kln['s'].upper()
        if len(AggTrades[symbol])%500==0:
            print('len(AggTrades) = %d' % len(AggTrades[symbol]))
        AggTrades[symbol].append({'_t': int(mess['T']), '_p': float(mess['p']), '_v': float(mess['q'])})               
    elif mess['e'] == 'kline':
        kln = mess['k']
        if kln['x'] is True: #if kln['s'] == symbol:
            symbol = kln['s'].upper()
            new_kln = { '_t': int(kln['t']), '_o': float(kln['o']), '_h': float(kln['h']), '_l': float(kln['l']), '_c': float(kln['c']), '_v': float(kln['q']) }
            SymKlns[symbol].append(new_kln)
            print(len(SymKlns[symbol]), '\t', symbol, '\t_t:', timestr(new_kln['_t']), '\t_h:', new_kln['_h'], \
                  '\t_l:', new_kln['_l'], '\t_c:', new_kln['_c'], '\t_v:', new_kln['_v'])
    elif mess['e'] ==   'ACCOUNT_UPDATE':
        pass

def on_error(ws, error):       
    print(error)
    ws.close()

def on_close(ws):
    print('\n' + barstr(text='Close Data Streaming') + '\n')

def on_open(ws, *args):      
    def data_stream(*args):
        params = [str.lower(ins) + str(s) for ins in insIds for s in stream] #, str.lower(sym_mk) + "@kline_1m"]
        print(params)
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1 })) #['btcusdt@kline_1m']
        while time.time() - start_time < run_time: #
            if len(SymKlns[insIds[0]]) % 30:
                client.keepalive_stream()   
        ws.close()
     
    t1 = threading.Thread(target=data_stream)        
    t1.start()

def header_print(testnet, client):
    t_server, t_local = client.timestamp(), time.time()*1000
    print('\tTestnet: %s' % str(testnet))
    print('\tServer Time at Start: %s' % timestr(t_server))
    print('\tLocal Time at Start: %s, \tOffset (local-server): %d ms\n' % (timestr(t_local), (t_local-t_server)))
    try:
        bal_st = pd.DataFrame(client.balance())
        bal_st['updateTime'] = [timestr(b) for b in bal_st['updateTime']]
        print('\nBeginning Balance Info: \n')
        print(bal_st)
    except Exception:
         print('\nFail to connect to client.balance: \n')

start_time = time.time()
run_time = 3*60 #second
testnet = True
if testnet:
    # Testnet
    apikey = ''
    scrkey = ''
else:
    # Binance
    apikey = ''
    scrkey = ''
insIds = [  'BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'LINKUSDT', 'XTZUSDT', 'LTCUSDT','DASHUSDT' ]
stream = ['@kline_1m']
BidAsk = {}
AggTrades = {}
SymKlns = {}
client = Client(apikey, scrkey, testnet=testnet)
client.change_position_mode(dualSide='true')
for symbol in insIds:
   client.change_leverage(symbol, 1)
   BidAsk[symbol] = []
   AggTrades[symbol] = []
   SymKlns[symbol] = []

print('\n' + barstr(text='Start Data Streaming') + '\n')     
header_print(testnet, client)
print('\nStream updating...')
listen_key = client.get_listen_key()
ws = websocket.WebSocketApp(f'{client.wss_way}{listen_key}',
                            on_message=on_message, 
                            on_error=on_error,
                            on_close=on_close)
ws.on_open = on_open
try:           
    ws.run_forever()
    ws.close()
except Exception:
    print("\tclosed on ERROR", fileout)

print('\n\tLocal Time at Close: %s \n' % timestr(time.time()*1000))
print(barstr(text='Elapsed time = {} seconds'.format(round(time.time()-start_time,2))))
print(barstr(text="", space_size=0))
os._exit(1)