# encoding: utf-8
import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import json
from datetime import datetime, timedelta

import requests
import pandas as pd
import matplotlib.pyplot as plt

from util.util import *

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import warnings
warnings.warn("", FutureWarning)


def data_concentrate(filename, time_interval = '1T'):
    file_path = os.path.join(os.path.abspath('.') + '/orders', file_name)
    with open(file_path) as meta_file:
        data = json.load(meta_file)
    trades = pd.DataFrame(data['finished_orders'].values())

    trades['time'] = [datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S') for x in trades['update_time'].tolist()]
    trades.set_index('time', inplace=True)

    trade_summary = get_trade_summary(trades, time_interval, 'price', 'filled')
    market_kline = get_kline_from_amber(trades['exchange'][0], trades['symbol'][0], str(trades.index.tolist()[0]), str(trades.index.tolist()[-1]), time_interval)
    df = pd.concat([trade_summary, market_kline], axis=1)
    
    return df

def data_visualization(df, figure_name='Strategy_Market_Size.png'):
    fig = plt.figure(figsize=(14, 8))
    fig.tight_layout()
    plt.title('Strategy & Market Size')

    ax = fig.add_subplot(111)
    ax2 = ax.twinx()

    ax.set_xlabel('Time')
    ax.set_ylabel('Strategy Quantity')
    ax2.set_ylabel('Market Quantity')

    ax.plot(df.index, df['filled'], label='Strategy',c='mediumseagreen')
    ax2.plot(df.index, df['volume'], label='Market', c='firebrick')

    ax.legend(loc=2)
    ax2.legend(loc=1)
    ax.grid()
    plt.savefig(os.path.abspath('.') + '/orders/' + figure_name)


if __name__ == '__main__':
    file_name = 'VWAP_Binance_ETHUSDT_20191031.json'
    figure_name = file_name.split('.')[0] + ' Strategy_Market_Size.png'
    data = data_concentrate(file_name)
    data_visualization(data, figure_name)
