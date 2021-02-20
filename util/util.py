# encoding: utf-8
import json
import random
import math
import io
import os
import sys
import time
from datetime import datetime, timedelta

from netifaces import interfaces, ifaddresses, AF_INET
import pandas as pd
import requests
import smtplib
import xlsxwriter
from pandas.api.types import is_numeric_dtype
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.mime.image import MIMEImage

from config.db_models import *
from config.enums import *
from config.config import *
from util.alioss import alioss
from util.logger import logger


def ip4_addresses():
    """
    Get IPv4 address
    :return: list, IPv4 address list
    """
    ip_list = []
    for interface in interfaces():
        adds = ifaddresses(interface)
        if AF_INET not in adds:
            continue
        for link in adds[AF_INET]:
            ip_list.append(link['addr'])
    return ip_list


def get_ip():
    """
    Get a unique IPv4 address
    :return: string
    """
    addrs = ip4_addresses()
    for ip in addrs:
        if ip.split('.')[2] != '0' and ip.split('.')[2] != '1':
            return ip


def get_pid():
    return os.getpid()


def update_strategy_conf(strategy, task):
    strategy['task_id'] = task['task_id']
    strategy['initial_balance'] = task['initial_balance'][f"{strategy['exchange']}|{strategy['account']}"]
    strategy['test_mode'] = task['test_mode']
    strategy['customer_id'] = task['customer_id']
    strategy['coin_config'] = task['coin_config'][strategy['exchange']]
    if 'start_time' not in strategy:
        strategy['start_time'] = task['start_time']
    if 'end_time' not in strategy:
        strategy['end_time'] = task['end_time']
    if 'trade_role' not in strategy:
        strategy['trade_role'] = task['trade_role']


def increase_reserved_amount(balance_ref, base, quote, direction, quantity, price):
    """
    update reserved amount by order response
    """
    if direction == Direction.SELL.value:
        balance_ref[base]["reserved"] += quantity
    else:
        balance_ref[quote]["reserved"] += quantity * price


def decrease_reserved_amount(balance_ref, base, quote, direction, quantity, price):
    """
    update reserved_amount by order_response
    """
    increase_reserved_amount(balance_ref, base, quote, direction, -1 * quantity, price)


def balance_management_common_process(balance_ref, resp, base, quote, origin_order):
    # 此时无需任何操作
    if resp["status"] == OrderStatus.PENDING.value:
        return False

    # 如果发单被拒, 则减去相应的资金占用量
    if resp["status"] == OrderStatus.REJECTED.value:
        decrease_reserved_amount(balance_ref, base, quote, resp["direction"], resp["original_amount"], resp["original_price"])
        return False

    # 其他情况都需要处理相应的资金占用量, Cancel, Fill/Partial Fill
    size_diff = resp["filled"] - origin_order["filled"]
    amount_diff = resp["filled"] * resp["avg_executed_price"] - origin_order["filled"] * origin_order["avg_price"]

    if resp["status"] in [OrderStatus.PARTIALLY_FILLED.value, OrderStatus.FILLED.value]:
        decrease_reserved_amount(balance_ref, base, quote, resp["direction"], size_diff, resp["original_price"])

    elif resp["status"] == OrderStatus.CANCELLED.value:
        size_remain = resp["original_amount"] - origin_order["filled"]
        decrease_reserved_amount(balance_ref, base, quote, resp["direction"], size_remain, resp["original_price"])

    factor = 1 if resp["direction"] == Direction.BUY.value else -1
    balance_ref[base]["total"] += size_diff * factor
    balance_ref[quote]["total"] -= amount_diff * factor

    balance_ref[base]["available"] = balance_ref[base]["total"] - balance_ref[base]["reserved"]
    balance_ref[quote]["available"] = balance_ref[quote]["total"] - balance_ref[quote]["reserved"]
    return True


def pdt_timestamp_readability(timestamp):
    """
     transfer timestamp to human readable format
    :param timestamp: 1571973060000
    :return:"2019-10-11 16:43:16"
    """
    if len(str(timestamp)) == 13:
        timestamp = timestamp / 1000
    timestamp = time.localtime(timestamp)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
    return timestamp


def summary_trades_base_quote(trades):
    """
    trades has columns filled, quote, avg_price
    :param trades:
    :return:
    """
    trades['quote'] = trades['avg_price'] * trades['filled']
    base_sum = trades['filled'].sum()
    quote_sum = trades['quote'].sum()
    return base_sum, quote_sum


def cal_orders(data, start_time, end_time, exchange_fee, service_fee=0.01, currency_type='Base'):
    """
    orders statistical info compute
    :param data: finished_orders, data structure ref finished_orders in strategy_base
    :param start_time: "2019-12-01 13:01:02"
    :param end_time: "2019-12-01 13:01:25"
    :param exchange_fee: 0.000675
    :param service_fee: default is 0.01
    :param currency_type: default is 'Base'
    :return:{
        'task_id': 'ICEBERG_Binance_BTCUSDT_20191201130101'
        'algorithm': 'ICEBERG',
        'account': 'trading',
        'direction': 'Buy',
        'exchange': 'Binance',
        'symbol': 'BTCUSDT',
        'coin_cost': 3660.1725078000004,
        'coin_net_get': 0.494665875,
        'coin_get': 0.49966249999999995,
        'total_size': 0.49999999999999994,
        'avg_price': 7320.345015600002
        'currency_type': 'Base'
    }

    """
    trades = pd.DataFrame(data.values())
    task_id = trades['notes'].values[0]['strategy_id']
    algo, exchange, symbol, _ = task_id.split('_')
    account = trades['account_id'].values[0]
    direction = trades['direction'].values[0]
    trades['datetime'] = [datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S') for x in
                          trades['update_time'].tolist()]
    trades.set_index('datetime', inplace=True)
    trades.sort_index(inplace=True)
    trades = trades.truncate(before=start_time, after=end_time)
    if trades.empty:
        return  {
                    'task_id': task_id,
                    'currency_type': currency_type,
                    'algorithm': algo,
                    'account': account,
                    'direction': direction,
                    'exchange': exchange,
                    'symbol': symbol,
                    'coin_cost': 0,
                    'coin_net_get': 0,
                    'coin_get': 0,
                    'total_size': 0,
                    'avg_price': 0
                }
    base_sum = 0
    quote_sum = 0
    if trades['symbol'].values[0] == symbol:
        # single symbol
        base_sum, quote_sum = summary_trades_base_quote(trades)
    else:
        # if t-*, needs to seperately statisc median and anchor
        # infer median & anchor symbol
        median, anchor= trades['symbol'].unique()
        if not median.startswith(symbol[0]):
            median, anchor = anchor, median
        median_trades = trades[trades['symbol'] == median]
        anchor_trades = trades[trades['symbol'] == anchor]
        base_median_sum, quote_median_sum  = summary_trades_base_quote(median_trades)
        base_anchor_sum, quote_anchor_sum  = summary_trades_base_quote(anchor_trades)

        if base_median_sum != 0 and base_anchor_sum != 0:
            direction = median_trades['direction'].values[0]
            if direction != anchor_trades['direction'].values[0]:
                # infer midcoin and quote_sum
                mid_coin = quote_anchor_sum if direction != anchor_trades['direction'].values[0] else base_anchor_sum
                quote_sum = base_anchor_sum if direction != anchor_trades['direction'].values[0] else quote_anchor_sum
            # if partially filled, get the minimal size pair
            base_sum = min(base_median_sum, mid_coin / quote_median_sum * base_median_sum)
            quote_sum = min(quote_sum, quote_median_sum / mid_coin * quote_sum)

    deal_amount = base_sum if currency_type == CurrencyType.BASE.value else quote_sum
    meta_quantity = base_sum if direction == Direction.BUY.value else quote_sum
    receive_coin_minus_exchange_fee = meta_quantity * (1 - exchange_fee)
    receive_coin_minus_ex_service_fee = receive_coin_minus_exchange_fee * (1 - service_fee)
    cal_info = {
        'task_id': task_id,
        'currency_type': currency_type,
        'algorithm': algo,
        'account': account,
        'direction': direction,
        'exchange': exchange,
        'symbol': symbol,
        'coin_cost': float(quote_sum) if direction == Direction.BUY.value else float(base_sum),
        'coin_net_get': float(receive_coin_minus_ex_service_fee),
        'coin_get': float(receive_coin_minus_exchange_fee),
        'total_size': float(deal_amount),
        'avg_price': 0 if base_sum == 0 else float(quote_sum / base_sum)
    }
    return cal_info



def avg_vol_ref_cal(exchange, symbol, start_time, end_time):
    """
     compute average history order volume in minutes, here compute the volume from kline data, which is from
     exchange kline api
    :param exchange:'Binance'
    :param symbol:'ETHBTC'
    :param start_time:"2019-10-11 16:43:16"
    :param end_time:"2019-10-12 16:43:16"
    :return: hist_avg_volume_in_minutes
    """
    exec_minutes = (str_to_datetime(end_time) - str_to_datetime(start_time)).total_seconds() / 60
    his_start = str_to_datetime(start_time) - timedelta(minutes=exec_minutes)
    ex_kline_instance = ExchangeKline()
    task = {
        'exchange': exchange,
        'symbol': [symbol, '', '']
    }
    kline_data = ex_kline_instance.exchange_kline(task, his_start, start_time)
    if not kline_data.empty:
        avg_vol_hour = kline_data['size'].sum() / exec_minutes * 60
    else:
        # for test
        avg_vol_hour = 0
    avg_vol_ref_minutes = avg_vol_hour / 60
    return avg_vol_ref_minutes


def cal_ob_avg_size(price_size_lists, depth=5):
    """
    cal cum size in first depth
    :param price_size_lists: [[1.2,3],...]
    :param depth: 5
    :return: cum size of price-size-lists
    """
    depth = min(len(price_size_lists), depth)
    size = [price_size_lists[i][1] for i in range(depth)]
    return sum(size) / depth if depth > 0 else 0


def cal_order_size_by_lkp(lksom, lksoc, p):
    """
    TO DO: can be tried in iceberg
    cal trade size of next minute, used in vwap
    :param lksom: trading volume of last minute in kline of market
    :param lksoc: trading volume of last minute in kline of customer
    :param p: the percentage we need to trade of the market, it's giver by the customer
    :return: trade_size of next minute
    """
    return (lksom - lksoc) * p


def cal_order_size_by_ob_tr(ob, tr, min_size, max_size):
    """
    cal order size by orderbook and trade timely, must be more than 2 times min order size and less than 0.5
    times max order size
    :param ob: orderbook accumulated size / orderbook depth
    :param tr: accumulated size in last one minute
    :param min_size: exchange pair minimal order size
    :param max_size: max order size, hard coded, set in config.py
    :return: timely order size
    """
    size = 0.7 * ob + 0.3 * tr
    size = min(size, max_size * 0.5)
    size = max(size, size, min_size * 2)
    return size + 0.3 * size * random.random()


def order_size_cal(avg_vol_ref, market_cum_vol, end_time, total_exec_vol, executed_vol):
    """
    compute next order size
    :param avg_vol_ref: history avg size, here sets time scale to 1 minute
    :param market_cum_vol: market cumulative volume from the start of algo, here compute from kline(1 min) data
    :param end_time: algo end time
    :param total_exec_vol: total quantity needs to be executed
    :param executed_vol: algo executed volume from start of algo
    :return: order size
    """
    end_time = str_to_datetime(end_time) if isinstance(end_time, str) else end_time
    target_execution_ratio = market_cum_vol / (avg_vol_ref * (end_time - datetime.now()).total_seconds() / 60 + (total_exec_vol - executed_vol) + market_cum_vol)
    real_exec_ratio = executed_vol / total_exec_vol
    order_size = (target_execution_ratio - real_exec_ratio) * total_exec_vol if target_execution_ratio > real_exec_ratio else 0
    return order_size


def get_maintain_amount(amount, price, m_base, mid_coin, amount_maintain_multiple=2):
    """
     compute maintain amount for anchor symbol
    :param amount: 3
    :param price:  3.1
    :param m_base: 'BTC'
    :param mid_coin: 'BTC'
    :param amount_maintain_multiple: default set to 2
    :return: 6
    """
    amount *= amount_maintain_multiple
    return amount if mid_coin == m_base else amount * price


def get_git_msg():
    """
    Get git msg with git head, git user, last push time and git commit
    :return: str
    """
    from git import Repo
    repo = Repo('.')
    head = repo.head.commit
    return f'{head.hexsha[:8]} {head.committer.name} {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(head.committed_date))} {head.message}'


def get_avg_price_size_from_ob(asks, depth=5):
    """
    get avg price from [[price,size],...] lists
    :param asks:[[price,size],...]
    :param depth:depth
    :return:
    """
    length = min(len(asks), depth)
    sum_quote = 0
    sum_base = 0
    for i in range(length):
        sum_base += asks[i][1]
        sum_quote += asks[i][0] * asks[i][1]
    return sum_quote / sum_base, sum_base / length


def str_to_datetime(string):
    """
    string = '2019_10_12 12:22:22'
    return = 2019-10-12 12:22:22.103000

    Transfer string to datetime

    :param string: str
    :return: class 'datetime.datetime'
    """
    for char in ['_', '-', ':', '.', ' ']:
        # remove special chart
        string = string.replace(char, '')
    if len(string) == 8:
        return datetime.strptime(string, '%Y%m%d')
    elif len(string) == 14:
        return datetime.strptime(string, '%Y%m%d%H%M%S')
    elif len(string) == 17:
        return datetime.strptime(string, '%Y%m%d%H%M%S%f')
    else:
        logger.info(f'wrong length of string:{len(string)}')
        return None


def get_volume_filter(config):
    if 'orderbook_threhold' in config:
        return config['orderbook_threhold']
    else:
        return None


def compute_direction(config, reference_symbol):
    """
    config = {
        "algorithm": "ICEBERG",
        "exchange": "Quoinex",
        "account": "ambertech",
        "symbol": ["NIIETH", "NII", "ETH"],
        "direction": "Sell",
        "currency_type": "Base",
        "total_size": 2000000,
        "trade_role": "Both",
        "price_threshold": 1.3e-05,
        "exchange_fee": 0.001,
        "execution_mode": "Passive",
        "test_mode": false,
        "start_time": "2019-10-11 16:43:16",
        "end_time": null,
        "initial_balance": {"NII": 552759703.3824672, "ETH": 0},
        "task_id": "ICEBERG_Quoinex_NIIETH_20191011164501",
        "coin_config": {
            "NIIETH": {
                "base_min_order_size": 0.0001,
                "quote_min_order_size": 0.001,
                "price_precision": 1e-08,
                "size_precision": 0.0001
            }
        },
        "customer_id": "amberai",
        "alarm": true
    }
    reference_symbol = 'median'

    Get the direction of reference_symbol when it comes to triangle trading

    :param config: dict
    :param reference_symbol: 'median' or 'anchor'
    :return: 'Buy' or 'Sell'
    """
    sb, sq = get_base_quote_currency_name(config, config['exchange'], 'symbol')
    rb, rq = get_base_quote_currency_name(config, config['exchange'], reference_symbol)
    # trade order direction
    rd = config['direction']
    if sb == rq or sq == rb:
        directions = [Direction.BUY.value, Direction.SELL.value]
        directions.remove(config['direction'])
        rd = directions[0]
    return rd


def get_total_balance(config, currency):
    """
    Get total balance of the currency
    """
    if currency in config:
        total = config[currency]['total']
        return float(total) if isinstance(total, str) else total
    else:
        return 0


def get_available_balance(config, currency):
    """
    Get available balance of the currency
    """
    if currency in config:
        available = config[currency]['available']
        return float(available) if isinstance(available, str) else available
    else:
        return 0


def compute_mid_coin(config):
    """
    Get the middle coin when it comes to triangle trading
    """
    sb, sq = get_base_quote_currency_name(config, config['exchange'], 'symbol')
    rb, rq = get_base_quote_currency_name(config, config['exchange'], 'anchor')
    mid_coin = rb if rb not in [sb, sq] else rq
    return mid_coin


def format_price(price, price_precision):
    """
    price = 0.005263
    price_precision = 0.00001
    return = 0.00526

    Adjust price to fixed accuracy

    :param price: float
    :param price_precision: float
    :return: float
    """
    if price_precision >= 1:
        return round(price / price_precision) * price_precision
    else:
        if '.' in str(price_precision):
            decimal = len(str(price_precision).split('.')[1])
        elif 'e' in str(price_precision) or 'E' in str(price_precision):
            temp = str(price_precision).replace('E', 'e')
            decimal = abs(int(temp.split('e')[-1]))
        else:
            print(f"wrong type of precision: {price_precision}")
            decimal = 10

        return round(price, decimal)


def format_amount(amount, amount_precision):
    """
    amount = 152.58
    amount_precision = 0.1
    return = 152.5

    Adjust amount to fixed accuracy and round down

    :param amount: float
    :param amount_precision: float
    :return: float
    """
    amount = math.floor(amount / amount_precision) * amount_precision
    amount = round(amount, get_decimal_from_precision(amount_precision))
    return amount


def amount_adjust(amount, amount_precision, min_size):
    """
        amount = 0.00365
        amount_precision = 0.0001
        min_size = 0.01534
        return = 0.0154

    Adjust amount to make it available to trade on the exchange

    :param amount: float,
    :param amount_precision: float, the accuracy of the price should match amount_precision
    :param min_size: float, adjusted amount >= min_size
    :return: float,
    """
    if amount <= 0:
        return 0

    if amount >= min_size + amount_precision:
        return amount

    amount = math.ceil(min_size / amount_precision) * amount_precision
    amount = round(amount, get_decimal_from_precision(amount_precision))
    return amount


def get_decimal_from_precision(precision):
    """
    precision = 0.001
    return = 3

    Get the number of decimal that match the accuracy

    :param precision: float
    :return: int
    """
    if precision >= 1:
        return 0

    if '.' in str(precision):
        decimal = len(str(precision).split('.')[1])
    else:
        decimal = int(str(precision)[-1])
    return decimal


def compute_executed_volume(orders):
    print(orders)


def get_base_quote_currency_name(config, exchange, symbol='symbol'):
    return config[symbol][1:3]


def get_min_order_size(config, exchange, symbol):
    return [config['coin_config'][symbol]['base_min_order_size'],
            config['coin_config'][symbol]['quote_min_order_size']]


def get_price_precision(config, exchange, symbol, contract_type='spot'):
    return config['coin_config'][symbol]['price_precision']


def get_amount_precision(config, exchange, symbol, contract_type='spot'):
    return config['coin_config'][symbol]['size_precision']


def get_anchor_price(m_price, a_price, s_base, m_base, s_quote, m_quote):
    """
    Get calculated price from middle price and anchor price when it comes to triangle trading
    """
    m_price = m_price if s_base == m_base else 1 / m_price
    a_price = a_price if s_quote == m_quote else 1 / a_price
    return m_price * a_price


def price_filter_by_volume(price_volume_list, volume_threshold=None):
    """
    get price filtered by volume_threshold from price_volume_list
    :param price_volume_list:[[price, size],...]
    :param volume_threshold: number type
    :return:
    """
    # the max inspect depth is set to 10
    price = 0.0
    size = 0.0
    order_filter_level = 10
    if volume_threshold:
        # set a filter to avoid small order size trick by sniffers, here we have the cumsum size
        depth = min(len(price_volume_list), order_filter_level)
        for i in range(depth):
            size += price_volume_list[i][1]
            price = price_volume_list[i][0]
            if size > volume_threshold:
                break
    else:
        price = price_volume_list[0][0]
        size = price_volume_list[0][1]
    return [price, size]


def get_datetime(timestamp):
    """
    timestamp = 20191012164822103
    return = 2019-10-12 16:48:22.103000

    Get datetime from human_readable_timestamp

    :param timestamp: str, int,
    :return: class 'datetime.datetime'
    """
    if not isinstance(timestamp, str):
        timestamp = str(timestamp)
    year = int(timestamp[0:4])
    month = int(timestamp[4:6])
    day = int(timestamp[6:8])
    hour = int(timestamp[8:10])
    minute = int(timestamp[10:12])
    second = int(timestamp[12:14])
    milisec = int(timestamp[14:17]) * 1000
    return datetime(year, month, day, hour, minute, second, milisec)


def market_data_validate(market_data, tolerance=3):
    """
    market_data = {
        "exchange": "Binance",
        "symbol": "MFTUSDT",
        "data_type": "quotestream",
        "contract_type": "spot",
        "metadata": {
            "bids": [[0.000982, 2121]...],
            "asks": [[0.000963, 11.62]...],
            "timestamp": "20191012153300000"
        },
        "update_type": "snapshot",
        "subscribed_orderbook_depth": 20,
        "timestamp: 20191012153300000
    }
    return = True

    Check if market data meets timeliness requirements

    :param market_data: dict,
    :param tolerance: float, the threshold for market data validation
    :return: bool, whether market data is valid
    """
    dt = get_datetime(market_data['timestamp'])
    now = datetime.now()
    return True if (now - dt).total_seconds() <= tolerance else False


def orderbook_price_filter(orderbook, amount, level=3):
    """
    orderbook = {
        "asks": [[0.0043, 1.3], [0.0049, 102.3]...],
        "bids": [[0.0041, 52.1], [0.0039, 100]...],
        "timestamp: 20191012153300000
    }
    amount = 10
    level = 3
    return = (0.0041, 0.0049)

    Filter orders in front of orderbook that the cumulative size are smaller than amount

    :param orderbook: dict,
    :param amount: float, the amount to be filtered
    :param level: int, the maximum level allowed to be filtered
    :return: tuple, bid0 and ask0 of orderbook after filter the amount
    """
    asks = orderbook['metadata']['asks']
    bids = orderbook['metadata']['bids']
    bprice = 0
    aprice = 0
    count = 0
    for i in range(level):
        count += bids[i][1]
        bprice = bids[i][0]
        if count >= amount:
            break

    count = 0
    for i in range(level):
        count += asks[i][1]
        aprice = asks[i][0]
        if count >= amount:
            break
    return bprice, aprice


def task_validate(task):
    """
    task = {
        "algorithm": "ICEBERG",
        "exchange": "Quoinex",
        "account": "ambertech",
        "symbol": ["NIIETH", "NII", "ETH"],
        "direction": "Sell",
        "currency_type": "Base",
        "total_size": 2000000,
        "trade_role": "Both",
        "price_threshold": 1.3e-05,
        "exchange_fee": 0.001,
        "execution_mode": "Passive",
        "test_mode": false,
        "start_time": "2019-10-11 16:43:16",
        "end_time": null,
        "initial_balance": {"NII": 552759703.3824672, "ETH": 0},
        "task_id": "ICEBERG_Quoinex_NIIETH_20191011164501",
        "coin_config": {
            "NIIETH": {
                "base_min_order_size": 0.0001,
                "quote_min_order_size": 0.001,
                "price_precision": 1e-08,
                "size_precision": 0.0001
            }
        },
        "customer_id": "amberai",
        "alarm": true
    }

    Check if the parameter of task is reasonable

    :param task: dict, receive from ui
    :return: tuple, (result, info)
    """
    if not isinstance(task['task_id'], str) or task['task_id'] == '':
        return False, f"task_id value error: {task['task_id']}"
    if not isinstance(task['exchange'], str) or task['exchange'] == '':
        return False, f"exchange value error: {task['exchange']}"
    if not isinstance(task['account'], str) or task['account'] == '':
        return False, f"account value error: {task['account']}"
    if not isinstance(task['symbol'], list) or len(task['symbol']) != 3:
        return False, f"symbol value error: {task['symbol']}"
    if not isinstance(task['initial_balance'], dict):
        return False, f"initial_balance type error, only support dict: {type(task['initial_balance'])}"
    for currency in task['initial_balance']:
        if not isinstance(task['initial_balance'][currency], (int, float)) or task['initial_balance'][currency] < 0:
            return False, f"initial_balance {currency} value error: {task['initial_balance'][currency]}"
    if not isinstance(task['direction'], str) or task['direction'] not in [Direction.BUY.value, Direction.SELL.value]:
        return False, f"direction value error, should be {Direction.BUY.value} or {Direction.SELL.value}, now is {task['direction']}"
    if not isinstance(task['currency_type'], str) or task['currency_type'] not in [CurrencyType.BASE.value, CurrencyType.QUOTE.value]:
        return False, f"currency_type value error: {task['currency_type']}, should be {CurrencyType.BASE.value} or {CurrencyType.QUOTE.value}"
    if task['trade_role'] == 'Maker' and task['execution_mode'] == 'Aggressive':
        return False, f"when execution_mode is Aggressive, trade_role can't be Maker"
    if not isinstance(task['total_size'], (int, float)) or task['total_size'] <= 0:
        return False, f"total_size value error: {task['total_size']}"

    if 'anchor_price' in task and task['anchor_price'] is not None:
        if task['price_threshold'] is not None:
            return False, "anchor_price and price_threshold can only be set one, another should be keep blank"
        if not isinstance(task['anchor_price'], (int, float)) or task['anchor_price'] < 0:
            return False, f"anchor_price value error: {task['anchor_price']}"

    if task['price_threshold'] is not None:
        if not isinstance(task['price_threshold'], (int, float)) or task['price_threshold'] < 0:
            return False, f"price_threshold value error: {task['price_threshold']}"

    if not isinstance(task['exchange_fee'], (int, float)) or task['exchange_fee'] < 0:
        return False, f"exchange_fee value error: {task['exchange_fee']}"
    if not isinstance(task['coin_config'], dict):
        return False, f"coin_config type error: {task['coin_config']}, shold be dict"
    for currency in task['coin_config']:
        if 'base_min_order_size' not in task['coin_config'][currency] or task['coin_config'][currency]['base_min_order_size'] < 0:
            return False, f"{currency} base_min_order_size value error: {task['coin_config'][currency]['base_min_order_size']}"
        if 'quote_min_order_size' not in task['coin_config'][currency] or task['coin_config'][currency]['quote_min_order_size'] < 0:
            return False, f"{currency} quote_min_order_size value error: {task['coin_config'][currency]['quote_min_order_size']}"
        if 'price_precision' not in task['coin_config'][currency] or task['coin_config'][currency]['price_precision'] < 0:
            return False, f"{currency} price_precision value error: {task['coin_config'][currency]['price_precision']}"
        if 'size_precision' not in task['coin_config'][currency] or task['coin_config'][currency]['size_precision'] < 0:
            return False, f"{currency} size_precision value error: {task['coin_config'][currency]['size_precision']}"
    if task['direction'] == Direction.SELL.value and task['currency_type'] == CurrencyType.BASE.value:
        if task['initial_balance'][task['symbol'][1]] < task['total_size']:
            return False, f"{task['symbol'][1]} initial_balance {task['initial_balance'][task['symbol'][1]]} should be bigger than total_size {task['total_size']}, when direction is sell and currency_type is base"
    if task['direction'] == Direction.BUY.value and task['currency_type'] == CurrencyType.QUOTE.value:
        if task['initial_balance'][task['symbol'][2]] < task['total_size']:
            return False, f"{task['symbol'][2]} initial_balance {task['initial_balance'][task['symbol'][2]]} should be more than total_size {task['total_size']}, when direction is buy and currency_type is quote"
    # 关于策略的task参数检测
    if task['algorithm'] in [Algorithms.TWAP.value, Algorithms.TRIANGLE_TWAP.value]:
        if not isinstance(task['start_time'], str) or task['start_time'] == '':
            return False, f"start_time value error: {task['start_time']}"
        if not isinstance(task['end_time'], str) or task['end_time'] == '':
            return False, f"end_time value error: {task['start_time']}"
        if task['start_time'] > task['end_time']:
            return False, f"start_time {task['start_time']} should be less tan end_time {task['end_time']}"
    if task['algorithm'] in [Algorithms.ICEBERG.value, Algorithms.TRIANGLE_ICEBERG.value]:
        if 'start_time' not in task or not isinstance(task['start_time'], str) or task['start_time'] == '':
            return False, f"start_time value error: {task['start_time']}"

    if task['algorithm'] in [Algorithms.VWAP.value]:
        if task['exchange'] not in VWAP_SUPPORT_EX:
            return False, f"VWAP don't support exchange {task['exchange']} now."

    if task['algorithm'] in [Algorithms.TRIANGLE_TWAP.value, Algorithms.TRIANGLE_ICEBERG.value]:
        if 'median' not in task or 'anchor' not in task:
            return False, "t-* algo should have keys median and anchor"
        else:
            if task['symbol'][0] == task['median'][0] or task['symbol'][0] == task['anchor'][0] or task['median'][0] \
                    == task['anchor'][0]:
                return False, 'anchor or median info config error'
            if (task['symbol'][1] != task['median'][1] and task['symbol'][1] != task['median'][2]) and (
                    task['symbol'][2] != task['median'][1] and task['symbol'][2] != task['median'][2]):
                return False, 'anchor or median info config error'
            if (task['symbol'][1] != task['anchor'][1] and task['symbol'][1] != task['anchor'][2]) and (
                    task['symbol'][2] != task['anchor'][1] and task['symbol'][2] != task['anchor'][2]):
                return False, 'anchor or median info config error'

    # 更多的检查条件
    return True, ''


def get_history_orders(task_id, strategy_id, start_time=False, end_time=False, trade_orders={}):
    file_path = os.path.join(ROOT_PATH, 'orders', f'{task_id}.json')
    if os.path.isfile(file_path):
        with open(file_path, 'r') as f:
            order_data = json.load(f)
            df = pd.DataFrame.from_dict(order_data['finished_orders'][strategy_id], orient='index')
            if strategy_id in  order_data['active_orders']:
                df_active = pd.DataFrame.from_dict(order_data['active_orders'][strategy_id], orient='index')
                df = pd.concat([df, df_active], axis=0)
    else:
        df = pd.DataFrame.from_dict(trade_orders, orient='index')
    if df.empty:
        return df
    df.rename(columns={'filled': 'filled_quantity'}, inplace=True)
    df['time'] = [datetime.strptime(x, '%Y-%m-%d %H:%M:%S') for x in df['update_time'].tolist()]
    df = df.sort_values(by='time', ascending=True)
    df = df.drop_duplicates(subset=['order_id'], keep='last')
    if start_time:
        df = df[df.time >= start_time]
    if end_time:
        df = df[df.time < end_time]
    df = df[df.filled_quantity > 0]
    df.set_index('time', inplace=True)

    return df


def create_execution_report(task, trade_orders={}):
    """
    task = {
        "algorithm": "ICEBERG",
        "exchange": "Quoinex",
        "account": "ambertech",
        "symbol": ["NIIETH", "NII", "ETH"],
        "direction": "Sell",
        "currency_type": "Base",
        "total_size": 2000000,
        "trade_role": "Both",
        "price_threshold": 1.3e-05,
        "exchange_fee": 0.001,
        "execution_mode": "Passive",
        "test_mode": false,
        "start_time": "2019-10-11 16:43:16",
        "end_time": null,
        "initial_balance": {"NII": 552759703.3824672, "ETH": 0},
        "task_id": "ICEBERG_Quoinex_NIIETH_20191011164501",
        "coin_config": {
            "NIIETH": {
                "base_min_order_size": 0.0001,
                "quote_min_order_size": 0.001,
                "price_precision": 1e-08,
                "size_precision": 0.0001
            }
        },
        "customer_id": "amberai",
        "alarm": true
    }
    start_time = "2019-10-10 12:00:00"
    end_time = "2019-10-11 16:00:00"

    Search database to create a execution summary of the task with market data

    :param task: dict, all parameters of the execution
    :param start_time: str, The time at which the execution summary begins
    :param end_time: str, The time at which the execution summary ends
    :return: oss link of report file
    """

    if not alioss.bucket:
        alioss.init()

    pd.set_option('precision', 8)

    start_time = task['start_time']
    if task["end_time"]:
        end_time = task["end_time"]
        # end_time = (datetime.strptime(task["end_time"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    all_links = {}
    file_list = []
    price_c = 'price'
    filled_quantity_c = 'filled_quantity'
    direction_c = 'Direction'
    average_price_c = 'Average Price'
    exchange_fee_c = 'Exchange Fee'
    commision_fee_c = 'Execution Commission'

    for strategy_id in task['task']['strategies']:
        strategy_param = task['task']['strategies'][strategy_id]
        strategy_data = get_history_orders(task['task_id'], strategy_id, start_time, end_time, trade_orders[strategy_id])
        if strategy_param['algorithm'] in [Algorithms.TRIANGLE_TWAP.value, Algorithms.TRIANGLE_ICEBERG.value]:
            anchor_trade = strategy_data[strategy_data.symbol.isin([strategy_param['anchor'][0]])]
            strategy_data = strategy_data[strategy_data.symbol.isin([strategy_param['median'][0]])]
            avg_anchor_price = (anchor_trade['price'] * anchor_trade['filled_quantity']).sum() / anchor_trade[
                'filled_quantity'].sum()

        df = pd.DataFrame()
        if strategy_data.empty:
            continue
        
        _, base, quote = strategy_param['symbol']
        direction = strategy_param['direction']
        exchange_fee = strategy_param['exchange_fee'] if 'exchange_fee' in strategy_param else 0
        commision_fee = strategy_param['service_fee'] if 'service_fee' in strategy_param else 0
        base_c = f'{base} Quantity'
        quote_c = f'{quote} Quantity'

        df[base_c] = strategy_data[filled_quantity_c].resample('60T').sum()
        df[base_c] = df[base_c] if direction == Direction.SELL.value else df[base_c] * (1 - exchange_fee)

        df[quote_c] = (strategy_data[price_c] * strategy_data[filled_quantity_c]).resample('60T').sum()
        df[quote_c] = df[quote_c] if direction == Direction.BUY.value else df[quote_c] * (1 - exchange_fee)

        df[average_price_c] = df[quote_c] / df[base_c]
        if strategy_param['algorithm'] in [Algorithms.TRIANGLE_TWAP.value, Algorithms.TRIANGLE_ICEBERG.value]:
            mid_coin = get_mid_coin_from_triangle_pair(strategy_param["symbol"], strategy_param["median"], strategy_param["anchor"])
            forward = True if strategy_param['anchor'][1] == mid_coin else False
            df[average_price_c] = df[average_price_c] * avg_anchor_price if forward else df[average_price_c] / avg_anchor_price
            df[quote_c] = (anchor_trade[price_c] * anchor_trade[filled_quantity_c]).resample(
                '60T').sum() if forward else anchor_trade[filled_quantity_c].resample('60T').sum()

        df[direction_c] = direction

        summary = {
            direction_c: direction,
            average_price_c: df[quote_c].sum() / df[base_c].sum(),
            base_c: df[base_c].sum(),
            quote_c: df[quote_c].sum()
        }

        df = df_add_summary(df, summary)

        # precision
        df = adjust_df_decimal(df)

        # resort column
        df = df[[direction_c, average_price_c, base_c, quote_c]]

        # market data
        if strategy_param['algorithm'] not in [Algorithms.TRIANGLE_TWAP.value, Algorithms.TRIANGLE_ICEBERG.value]:
            kline = ExchangeKline()
            try:
                formated_st = time.strftime('%Y-%m-%d %H:00:00', time.strptime(start_time, '%Y-%m-%d %H:%M:%S'))
                market_df = kline.exchange_kline(strategy_param, formated_st, end_time)
                df = pd.concat([df, market_df], axis=1)
            except Exception as e:
                logger.error(e)

        df[exchange_fee_c] = exchange_fee
        df[commision_fee_c] = commision_fee

        st = replace_special_chart_in_string(start_time)
        et = replace_special_chart_in_string(end_time)

        email_file_name = "{} Execution_Summary_from_{}_to_{}.xlsx".format(strategy_id, st, et)
        df.fillna(' ', inplace=True)
        write_df_to_excel(df, email_file_name, True, False)

        filename = f'{"TEST_" if task["test_mode"] else "EAAS_"}{task["task_id"]}_{strategy_id}.xlsx'
        link = alioss.sign_url(filename)
        alioss.delete_if_exist(filename)

        import io
        iofile = io.BytesIO()
        write_df_to_excel(df, filename, True, False, iofile)
        csv_string = iofile.getvalue()
        alioss.file_append(filename, 0, csv_string)
        all_links[strategy_id] = link
        file_list.append(email_file_name)
    return all_links, file_list


def get_formated_decimal_from_number(number, max_display=8, separator=False):
    """
    number = 623.66612102
    return = 5

    Get a reasonable precision for a number or adjust precision to match the ui display

    :param number: float,
    :param max_display: int, optional, default 8, commonly used display length
    :param separator: bool, optional, default False, whether display with separator like ","
    :return:
    """
    number = abs(number)
    if number >= 1:
        if '.' in str(number) and 'e' not in str(number):
            decimal = max_display - len(str(number).split('.')[0])
        elif 'e' in str(number):
            decimal = max_display - int(str(number)[-1])
        else:
            decimal = max_display - len(str(number))

        if separator:
            # using ',' formate
            if number >= 1e3:
                decimal = decimal - 1
            if number >= 1e9:
                decimal = decimal - 1
        decimal = 0 if decimal < 0 else decimal
    else:
        decimal = max_display - 1 if separator else max_display
    return decimal


def get_mid_coin_from_triangle_pair(symbol, median, quote):
    """
    Get the middle coin when it comes to triangle trading
    """
    mid_coin = False
    for currency in [median[1], median[2]]:
        if currency != symbol[1] and currency != symbol[2]:
            mid_coin = currency
            break
    return mid_coin


def get_price_offset_from_prices(direction, ask0, bid0, price_precision, execution_mode):
    """
    direction = 'Sell'
    ask0 = 198.56
    bid0 = 195.36
    price_precision = 0.01
    execution_mode = 'Passive'
    return = -0.0000503626

    Get the offset relative to the reference price, affected by direction and execution_mode

    :param direction: 'Sell' or 'Buy'
    :param ask0: float,
    :param bid0: float,
    :param price_precision: float,
    :param execution_mode: 'Passive' or 'Aggressive'
    :return: the offset relative to the reference price
    """
    if execution_mode == "Passive":
        offset = price_precision / bid0
    elif execution_mode == "Aggressive":
        offset = (ask0 - bid0) / bid0 / 2
    else:
        offset = 0
    offset = -1 * offset if direction == Direction.SELL.value else offset
    return offset


def send_execution_report_email(task, trade_orders):
    """
        task = {
        "algorithm": "ICEBERG",
        "exchange": "Quoinex",
        "account": "ambertech",
        "symbol": ["NIIETH", "NII", "ETH"],
        "direction": "Sell",
        "currency_type": "Base",
        "total_size": 2000000,
        "trade_role": "Both",
        "price_threshold": 1.3e-05,
        "exchange_fee": 0.001,
        "execution_mode": "Passive",
        "test_mode": false,
        "start_time": "2019-10-11 16:43:16",
        "end_time": null,
        "initial_balance": {"NII": 552759703.3824672, "ETH": 0},
        "task_id": "ICEBERG_Quoinex_NIIETH_20191011164501",
        "coin_config": {
            "NIIETH": {
                "base_min_order_size": 0.0001,
                "quote_min_order_size": 0.001,
                "price_precision": 1e-08,
                "size_precision": 0.0001
            }
        },
        "customer_id": "amberai",
        "alarm": true
    }

    st = False
    et = False

    Send execution report email base on database record

    :param task: dict, receive from ui
    :param s_t: bool, str, the time at which the execution summary begins
    :param e_t: bool, str, the time at which the execution summary ends
    """



    _, file_list = create_execution_report(task, trade_orders)

    st = replace_special_chart_in_string(task['start_time'])
    et = replace_special_chart_in_string(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    subject = "{} Execution_Summary_from_{}_to_{}".format(task["task_id"], st, et)
    email_center = EmailCenter()
    email_center.send_email_with_several_file(subject, file_list)

def replace_special_chart_in_string(string):
    """
    string = '2019-10-14 12:55:12'
    return = '20191014125512'

    Replace specific string with ''

    :param string: str,
    :return: str,
    """
    for char in ['_', '-', ':', '.', ' ']:
        string = string.replace(char, '')
    return string


def save_orders(order_data, file_name):
    log_dir = os.path.join(ROOT_PATH, 'orders')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    with open(os.path.join(log_dir, file_name), 'w') as f:
        json.dump(order_data, f)


def get_trade_summary(trade_history, time_interval, price_key, quantity_key):
    """
    trade_history = DataFrame
    time_interval = '1T'
    price_key = 'price'
    quantity_key = 'filled'

    Get the minute kline from amber service

    :param trade_history: dateframe
    :param time_interval: string, example: '1T', '60T', '1D', means: 1 minute, 1 hour, 1day
    :param price_key: string
    :param quantity_key: string
    :return: DataFrame
    """

    if trade_history.empty or price_key not in trade_history.columns or quantity_key not in trade_history.columns:
        return pd.DataFrame(columns={quantity_key: "", price_key: ""}, index=[0])

    df = pd.DataFrame()
    df[quantity_key] = trade_history[quantity_key].resample(time_interval).sum()
    df[price_key] = (trade_history[price_key] * trade_history[quantity_key]).resample(time_interval).sum() / trade_history[quantity_key].resample(time_interval).sum()
    df.dropna(axis=0, inplace=True)
    return df


def get_kline_from_amber(exchange, symbol, kline_start, kline_end, time_interval = '1T'):
    """
    exchange = 'Huobi'
    symbol = 'ETHUSDT'
    kline_start = '2019-10-21 11:00:00'
    kline_end = '2019-10-21 12:00:00'
    time_interval = '1T'

    Get the minute kline from amber service

    :param exchange: string, lowercase
    :param symbol: string, lowercase,
    :param s_date: string
    :param e_date: string
    :param time_interval: string, example: '1T', '60T', '1D', means: 1 minute, 1 hour, 1day
    :return: dateframe
    """

    s_date = replace_special_chart_in_string(kline_start.split(' ')[0])
    e_date = replace_special_chart_in_string(kline_end.split(' ')[0])
    if s_date == e_date:
        e_date = datetime.strftime(datetime.strptime(s_date, "%Y%m%d") + timedelta(hours=24), '%Y%m%d')

    if symbol in EAAS_SYMBOL_MAP:
        symbol = EAAS_SYMBOL_MAP[symbol]
    formate_symbol = '{}_{}'.format(SYMBOL_BASE_QUOTE[symbol][0], SYMBOL_BASE_QUOTE[symbol][1])

    url = 'http://47.75.57.213:3001/api/kline?exchange={}&symbol={}&contract_type=spot&from={}&to={}'.format(exchange.lower(), formate_symbol.lower(), s_date, e_date)
    res = requests.get(url, timeout=3)
    data = res.json()
    market_kline = pd.DataFrame(data['data'])

    market_kline['time'] = [datetime.strptime(str(x), '%Y-%m-%dT%H:%M:00.000Z') + timedelta(hours=8) for x in market_kline['time'].tolist()]
    market_kline = market_kline[(market_kline.time >= (datetime.strptime(kline_start, "%Y-%m-%d %H:%M:%S")) - timedelta(minutes=1)) & (market_kline.time <= datetime.strptime(kline_end, "%Y-%m-%d %H:%M:%S"))]
    market_kline.set_index('time', inplace=True)

    kline = pd.DataFrame()
    kline['high'] = market_kline['high'].resample(time_interval).max()
    kline['low'] = market_kline['low'].resample(time_interval).min()
    kline['open'] = market_kline['open'].resample(time_interval).first()
    kline['close'] = market_kline['close'].resample(time_interval).last()
    kline['volume'] = market_kline['volume'].resample(time_interval).sum()

    return kline


def keep_order_to_alioss(order, file_name):
    link = alioss.sign_url(file_name)
    return link

def adjust_df_decimal(df, max_data_length=8):
    for column_name in df.columns:
        if is_numeric_dtype(df[column_name]):
            dec = get_formated_decimal_from_number(df[column_name].mean(), max_data_length)
            df[column_name] = round(df[column_name], dec)
    return df


def df_add_summary(df, summary, summary_name='Summary'):
    df_t = pd.DataFrame(summary, index=[summary_name])
    df = pd.concat([df, df_t], axis=0)
    return df


def check_precison_of_number(number, precision):
    result = min(math.ceil(number / precision) - number / precision, number / precision - math.floor(number / precision))
    if result > 1e-8:
        return False
    else:
        return True


def create_export_statistics(data, ors_stat_info, current_price, price_limit, base, quote, test_mode, total_size):
    if not alioss.bucket:
        alioss.init()

    statis_review = {
        # 'Start Date': datetime.strptime(data['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y'),
        'Base CCY': base if data['direction'] == 'Sell' else quote,
        'Direction': data['direction'],
        'Execution Amount(Base CCY)': ors_stat_info['coin_cost'],
        'Order Type': data['task_id'].split('_')[0],
        'Price Limit': price_limit if price_limit is not None else '-',
        'Settle CCY': base if data['direction'] == 'Buy' else quote,
        'Remain AMT(BASE CCY)': total_size - ors_stat_info['coin_cost'],
        'Avg Px': ors_stat_info['avg_price'],
        'Current Px': current_price,
        'Fee(%)': 0 if 'service_fee' not in data or data['service_fee'] is None else data['service_fee'],
        'Settlement AMT': ors_stat_info['coin_net_get'],
        'Last Updated Time': (datetime.strptime(data['start_time'], '%Y-%m-%d %H:%M:%S') - timedelta(days=1)).strftime('%d/%m/%Y'),
    }

    table_name = 'Execution Status'

    df = pd.DataFrame(statis_review, index=[datetime.strptime(data['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')])

    filename = f'{"TEST_" if test_mode else "EAAS_"}{data["task_id"]}.xlsx'
    link = alioss.sign_url(filename)
    alioss.delete_if_exist(filename)

    import io
    iofile = io.BytesIO()
    write_df_to_excel(df, filename, False, table_name, iofile, table_key='export_statistics', index_name='Start Date')
    csv_string = iofile.getvalue()
    alioss.file_append(filename, 0, csv_string)
    return link


class ExchangeKline:
    def __init__(self):
        pass

    def exchange_kline(self, task, start_time, end_time):
        """
        task = {
            "algorithm": "ICEBERG",
            "exchange": "Quoinex",
            "account": "ambertech",
            "symbol": ["NIIETH", "NII", "ETH"],
            "direction": "Sell",
            "currency_type": "Base",
            "total_size": 2000000,
            "trade_role": "Both",
            "price_threshold": 1.3e-05,
            "exchange_fee": 0.001,
            "execution_mode": "Passive",
            "test_mode": false,
            "start_time": "2019-10-11 16:43:16",
            "end_time": null,
            "initial_balance": {"NII": 552759703.3824672, "ETH": 0},
            "task_id": "ICEBERG_Quoinex_NIIETH_20191011164501",
            "coin_config": {
                "NIIETH": {
                    "base_min_order_size": 0.0001,
                    "quote_min_order_size": 0.001,
                    "price_precision": 1e-08,
                    "size_precision": 0.0001
                }
            },
            "customer_id": "amberai",
            "alarm": true
        }
        start_time = '2019-10-12 12:00:00'
        end_time = '2019-10-12 14:00:00'

        return =
                                    Open         High           Low         Close
        2019-10-12 12:00:00         189.2        191.3          188.5        189.4
        2019-10-12 13:00:00         189.1        191.5          188.2        189.4

        Get Kline data from exchange with open, high, low, close and volume data, interval=1h

        :param task: dict
        :param start_time: str, kline start time
        :param end_time: str, kline end time
        :return: dataframe
        """
        df = pd.DataFrame
        exchange = task['exchange']
        symbol, base, quote = task['symbol']
        if exchange not in EXCHANGE_KLINE_AVAILABLE:
            return df
        url = EXCHANGE_KLINE_AVAILABLE[exchange]["URL"]
        if exchange == "Binance":
            kline_data = self.binance_kline(url, symbol)
        elif exchange == "Huobi":
            kline_data = self.huobi_kline(url, symbol)
        elif exchange == "Coinone":
            kline_data = self.coinone_kline(url, base)
        elif exchange == "Gateio":
            kline_data = self.gateio_kline(url, base, quote)
        elif exchange == "Cointiger":
            kline_data = self.cointiger_kline(url, symbol)
        elif exchange == "Upbit":
            kline_data = self.upbit_kline(url, base, quote)
        elif exchange == "Bitfinex":
            kline_data = self.bitfinex_kline(url, symbol)
        elif exchange == "Bitmax":
            kline_data = self.bitmax_kline(url, base, quote)
        elif exchange == "Bittrex":
            kline_data = self.bittrex_kline(url, base, quote)
        elif exchange == "Coinbase":
            kline_data = self.coinbase_kline(url, base, quote)
        elif exchange == "Okex":
            kline_data = self.okex_kline(url, base, quote)
        else:
            return df
        df = self.kline_cut(kline_data, start_time, end_time)
        return df

    @staticmethod
    def kline_cut(df, start_time, end_time):
        df['time'] = [datetime.fromtimestamp(timestamp) for timestamp in df['time'].tolist()]
        s_t = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') if isinstance(start_time, str) else start_time
        e_t = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') if isinstance(end_time, str) else end_time
        df = df[(df.time >= s_t) & (df.time < e_t)]

        df.set_index('time', inplace=True)
        df = df.sort_index(axis=0, ascending=True)
        if 'size' in df.columns:
            df = df[['Open', 'High', 'Low', 'Close', 'size']]
        else:
            df = df[['Open', 'High', 'Low', 'Close']]
        df = df.astype("float")
        return df

    @staticmethod
    def binance_kline(url, symbol):
        url = url.format(symbol)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data, columns=['time', 'Open', 'High', 'Low', 'Close', 'size', '', '', '', '', '', ''])
        df['time'] = (df['time'] / 1000).astype('int')
        df = df[['time', 'Open', 'High', 'Low', 'Close', 'size']]
        return df

    @staticmethod
    def huobi_kline(url, symbol):
        url = url.format(symbol.lower())
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data'], columns=['id', 'open', 'close', 'low', 'high', 'amount', 'vol', 'count'])
        df['time'] = (df['id']).astype('float')
        df['time'] = df['time'].astype('int')
        df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'amount': 'size'})
        df = df[['time', 'Open', 'High', 'Low', 'Close', 'size']]
        return df

    @staticmethod
    def coinone_kline(url, base):
        url = url.format(base.lower())
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data'], columns=['DT', 'Open', 'Low', 'High', 'Close', 'Volume', 'Adj_Close'])
        df['time'] = (df['DT']).astype('float')
        df['time'] = (df['time'] / 1000).astype('int')
        df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'})
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def gateio_kline(url, base, quote):
        url = url.format(base.lower() + "_" + quote.lower())
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data'], columns=['time', 'size', 'Open', 'High', 'Low', 'Close'])
        df['time'] = (df['time']).astype('float')
        df['time'] = (df['time'] / 1000).astype('int')
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def cointiger_kline(url, symbol):
        url = url.format(symbol.lower())
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data']['kline_data'])
        df['time'] = (df['id']).astype('int')
        df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'})
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def upbit_kline(url, base, quote):
        url = url.format(quote + "-" + base)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data)
        df['time'] = [int(time.mktime(time.strptime(t, '%Y-%m-%dT%H:%M:%S+00:00')) + 8 * 60 * 60) for t in
                      df['candleDateTime'].tolist()]
        df.drop_duplicates(subset='time', inplace=True)
        df = df.rename(columns={'openingPrice': 'Open', 'highPrice': 'High', 'lowPrice': 'Low', 'tradePrice': 'Close'})
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def bitfinex_kline(url, symbol):
        len_sym = len(symbol)
        if len_sym == 7 and symbol[len_sym - 4: len_sym] == 'USDT':
            symbol = f't{symbol[0:3]}UST'
        elif len_sym == 6 and symbol[len_sym - 3: len_sym] in ['USD', 'ETH', 'BTC', 'DAI', 'GBP', 'EUR', 'JPY']:
            symbol = f't{symbol}'
        else:
            symbol = EXCHANGE_KLINE_AVAILABLE['Bitfinex'][symbol]

        url = url.format(symbol)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data, columns=['time', 'Open', 'High', 'Low', 'Close', 'size'])
        df['time'] = (df['time'] / 1000).astype('int')
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def bitmax_kline(url, base, quote):
        st_ts = int(time.time() - 10 * 24 * 60 * 60)
        et_ts = int(time.time())
        url = url.format(base + '-' + quote, st_ts * 1000, et_ts * 1000)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data'])
        df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 't': 'time', 'v': 'size'}, inplace=True)
        df['time'] = df['time'].astype('float')
        df['time'] = (df['time'] / 1000).astype('int')
        df = df[['time', 'Open', 'High', 'Low', 'Close', 'size']]
        return df

    @staticmethod
    def bittrex_kline(url, base, quote):
        url = url.format(quote + '-' + base)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['result'])
        df.rename(columns={'O': 'Open', 'H': 'High', 'L': 'Low', 'C': 'Close', 'T': 'time'}, inplace=True)
        df['time'] = [int(time.mktime(time.strptime(x, '%Y-%m-%dT%H:%M:%S')) + 8 * 3600) for x in df["time"].tolist()]
        df = df[['time', 'Open', 'High', 'Low', 'Close']]
        return df

    @staticmethod
    def coinbase_kline(url, base, quote):
        url = url.format(base + '-' + quote)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data, columns=['time', 'Open', 'High', 'Low', 'Close', 'size'])
        df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 't': 'time', 'v': 'size'}, inplace=True)
        df = df[['time', 'Open', 'High', 'Low', 'Close', 'size']]
        return df

    @staticmethod
    def okex_kline(url, base, quote):
        url = url.format(base + '-' + quote)
        res = requests.get(url, timeout=TIMEOUT)
        data = res.json()
        df = pd.DataFrame(data['data'], columns=['time', 'Open', 'High', 'Low', 'Close', 'size'])
        df['time'] = [int(time.mktime(time.strptime(t, '%Y-%m-%dT%H:%M:%S.000Z')) + 8 * 3600) for t in
                      df["time"].tolist()]
        df = df[['time', 'Open', 'High', 'Low', 'Close', 'size']]
        return df


class EmailCenter:
    def __init__(self):
        mail_host = EMAIL_INFO['mail_host']
        mail_user = EMAIL_INFO['mail_user']
        mail_pass = EMAIL_INFO['mail_pass']

        self.server = smtplib.SMTP_SSL(mail_host)
        self.server.login(mail_user, mail_pass)
        self.sender = EMAIL_INFO["sender"]
        self.receiver = EMAIL_INFO["receivers"]

        self.msg = MIMEMultipart()
        self.msg['From'] = self.sender
        self.msg['To'] = self.sender

    def insert_image(self):
        pass

    def send_email(self, subject, file_list, receiver_key):
        """
        subject= 'Trade Summary from start_time to end_time
        file_list = ['file_name']
        receiver_key = 'test'

        Send email with attach and content from dataframe

        :param subject: str, email subject
        :param file_list: list, email attach and content
        :param receiver_key: str, grouping of recipients
        """
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = self.sender

        for file_name in file_list:
            file_full_name = file_name + '.xlsx'
            df = pd.read_excel(os.path.join(DIR_PATH, file_name + '.xlsx'), index_col=0)
            df_html = df.to_html(escape=False, index=True, sparsify=False, border=2, index_names=False, header=True,
                                 col_space=10, na_rep=' ', justify='center')
            # style = 'style="width:50%;max-width:100%;min-width:90%;margin-bottom: 20px;table-layout:fixed;word-wrap:break-word;" '
            # to_find = '<table '
            # destinate = df_html.find(to_find, )
            # df_html = df_html[0: destinate + len(to_find)] + style + df_html[destinate + len(to_find):]
            part = MIMEText(df_html, "html", "utf-8")
            msg.attach(part)

            attach = MIMEText(open(os.path.join(DIR_PATH, file_full_name), 'rb').read(), 'base64', 'utf-8')
            attach["Content-Type"] = 'application/octet-stream'
            attach["Content-Disposition"] = 'attachment; filename={}'.format(file_full_name)
            msg.attach(attach)
        text = msg.as_string()
        receiver = self.receiver[receiver_key]
        self.server.sendmail(self.sender, receiver, text)

    def send_email_with_several_file(self, subject, file_list):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = json.dumps(self.receiver)
        for file_name in file_list:
            mail_msg = """
                <html>
                     <head></head>
                     <body>
                         <p>
                         <b>
                             %s:
                             \n <br>
                         </b>
                         </p>
                     </body>
                 </html>
                 """ % file_name.split('.')[0]
            part = MIMEText(mail_msg, "html", "utf-8")
            msg.attach(part)
            df = pd.read_excel(os.path.join(DIR_PATH, file_name + '.xlsx'), index_col=0)
            df_html = df.to_html(escape=False, index=True, sparsify=False, border=2, index_names=False, header=True,
                                 col_space=10, na_rep=' ', justify='center')
            part = MIMEText(df_html, "html", "utf-8")
            msg.attach(part)

            attach = MIMEText(open(os.path.join(DIR_PATH, file_name), 'rb').read(), 'base64', 'utf-8')
            attach["Content-Type"] = 'application/octet-stream'
            attach["Content-Disposition"] = 'attachment; filename={}'.format(file_name)
            msg.attach(attach)

        text = msg.as_string()
        self.server.sendmail(self.sender, self.receiver, text)


def write_df_to_excel(df, file_name, have_summary, title, save_path=False, table_key='report', index_name='Time'):
    file_name = file_name.split('.')[0] + '.xlsx'
    rows = df.shape[0]
    columns = df.shape[1]
    excel_rows = rows + 1 if not title else rows + 2
    excel_columns = columns + 1

    if save_path:
        writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
    else:
        writer = pd.ExcelWriter(os.path.join(DIR_PATH, file_name), engine='xlsxwriter')
    workbook = writer.book

    # set title format
    title_format = workbook.add_format({
        'bold': True,
        'font_name': 'Bahnschrift',  # 'Futura',
        'font_size': 10,
        'font_color': 'white',
        'text_wrap': False,
        'align': 'center_across',
        'valign': 'vcenter',
        'bg_color': 'black',
        'border': 2,
        'text_wrap': True
    })

    col_name_format = workbook.add_format({
        'bold': True,
        'font_name': 'Bahnschrift',  # 'Futura',
        'font_size': 10,
        'font_color': 'black',
        'text_wrap': False,
        'align': 'center_across',
        'valign': 'vcenter',
        'bg_color': 'white',
        'border': 2,
        'text_wrap': True
    })

    bold_color = 'black'
    bold_style = 2
    general_num_format= ''
    pos_num_format = '#,##0.00'
    neg_num_format = '(#,##0.00)'

    header_format = [
        workbook.add_format({'bold': False, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'bg_color': '#F3F3F3', 'left': bold_style, 'left_color': bold_color}),
        workbook.add_format({'bold': False, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'left': bold_style, 'left_color': bold_color}),
        workbook.add_format({'bold': False, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'bg_color': '#F3F3F3', 'left': bold_style, 'left_color': bold_color, 'bottom':bold_style, 'bottom_color':bold_color}),
        workbook.add_format({'bold': False, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'left': bold_style, 'left_color': bold_color, 'bottom': bold_style, 'bottom_color':bold_color}),
        workbook.add_format({'bold': True, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'left': bold_style, 'left_color': bold_color, 'bottom': bold_style, 'bottom_color':bold_color}),
        workbook.add_format({'bold': True, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'bottom': bold_style, 'bottom_color': bold_color, 'font_size': 10}),

        workbook.add_format({'bold': True, 'valign': 'vcenter', 'font_name': 'Bahnschrift', 'text_wrap': False,
                             'align': 'center', 'right': bold_style, 'right_color': bold_color, 'bottom': bold_style, 'bottom_color': bold_color, 'font_size': 10}),
        workbook.add_format({'num_format': pos_num_format, 'valign': 'vcenter', 'font_name': 'Bahnschrift',
                             'align': 'center', 'bottom': bold_style, 'bottom_color': "white", 'right_color': "white", 'font_size': 10}),
        workbook.add_format({'num_format': pos_num_format, 'valign': 'vcenter', 'font_name': 'Bahnschrift',
                             'align': 'center', 'bg_color': '#F3F3F3', 'right': bold_style, 'right_color': "#F3F3F3", 'font_size': 10}),
        workbook.add_format({'num_format': general_num_format, 'valign': 'vcenter', 'font_name': 'Bahnschrift',
                        'align': 'center', 'bottom': bold_style, 'bottom_color': "white", 'right_color': "white", 'font_size': 10}),
        workbook.add_format({'num_format': general_num_format, 'valign': 'vcenter', 'font_name': 'Bahnschrift',
                        'align': 'center', 'bg_color': '#F3F3F3', 'right': bold_style, 'right_color': "#F3F3F3", 'font_size': 10}),
        ]


    df.to_excel(writer)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    worksheet.set_column(0, 0, 20)
    column_width = 12 if have_summary else 15
    worksheet.set_column(1, excel_columns - 2, column_width)
    worksheet.set_column(excel_columns - 1, excel_columns - 1, column_width)

    # set height
    for i in range(excel_rows):
        worksheet.set_row(i, 18)
    if table_key == 'export_statistics':
        worksheet.set_row(1, 36)
    # write title
    if title:
        worksheet.merge_range(0, 0, 0, excel_columns - 1, title, title_format)
    # write columns
    for i in range(excel_columns):
        if i == 0:
            col_name = index_name
        else:
            col_name = df.columns.tolist()[i - 1]
        if title:
            worksheet.write(1, i, col_name, title_format)
        else:
            worksheet.write(0, i, col_name, title_format)

    df.fillna(0, inplace=True)

    for i in range(rows):
        cell_format = header_format[8] if i % 2 == 0 else header_format[7]
        if not title:
            if i == rows - 1 and have_summary:
                for j in range(columns):
                    value = df.iloc[i, j]
                    worksheet.write(i + 1, j + 1, value, title_format)
            else:
                for j in range(columns):
                    value = df.iloc[i, j]
                    if isinstance(value, float) and value < 0.1:
                        cell_format = header_format[10] if i % 2 == 0 else header_format[9]
                    worksheet.write(i + 1, j + 1, value, cell_format)
            worksheet.write(i + 1, 0, str(df.index.tolist()[i]), cell_format)
        else:
            if i == rows - 1 and have_summary:
                for j in range(columns):
                    value = df.iloc[i, j]
                    worksheet.write(i + 2, j + 1, value, title_format)
            else:
                for j in range(columns):
                    value = df.iloc[i, j]
                    if isinstance(value, float) and value < 0.1:
                        cell_format = header_format[10] if i % 2 == 0 else header_format[9]
                    worksheet.write(i + 2, j + 1, value, cell_format)
            worksheet.write(i + 2, 0, str(df.index.tolist()[i]), cell_format)

        if have_summary:
            worksheet.write(rows, 0, str(df.index.tolist()[-1]), title_format)

    writer.save()
