# encoding: utf-8
#
import random
from datetime import datetime
import json

from config.enums import *
from strategy.strategy_base import StrategyBase
from util.logger import logger
from util.util import *


class Vwap(StrategyBase):
    def __init__(self):
        super().__init__()
        self.trading = True
        self.last_kline_timestamp = 0
        self.last_kline_vol = 0  # latest kline volume, used to update kline data;
        self.last_kline_size_of_market = 0  # last kline size of market in minute
        self.last_kline_size_of_customer = 0  # last kline size of customer in minute
        self.market_cum_vol = 0  # market data cumulative volume since algo start, including client volume
        self.customer_cum_vol_exclude_last_minute = 0  # customer cumulative volume since algo start
        self.cum_vol_not_used_of_market = 0  # market cumulative volume, that is not used for minimal order size
        self.mkr_unused_tsp = 0  # market volume not used timestamp
        self.market_data = {}  # latest market data
        self.trade_data = {}  # trade data store
        self.last_order_time = datetime.now()  # latest send order time
        self.order_interval = 5  # order interval is at least 5s
        self.avg_vol_ref_mins = 0  # history avg size in minutes

    def on_init(self, config, task, master_ptr):
        """
        reference to strategy_base
        """
        super().on_init(config, task, master_ptr)
        if self.task['order_mode'] == 'time_based':
            self.avg_vol_ref_mins = avg_vol_ref_cal(self.task['exchange'], self.task['symbol'][0],
                                                    self.task['start_time'],
                                                    self.task['end_time'])
            logger.info(f"avg_vol_ref_mins: {self.avg_vol_ref_mins}")

    def on_book(self, market_data):
        # 调用公用的行情处理
        super().on_book(market_data)

    def on_kline_ready(self, kline):
        timestamp = kline['metadata']['timestamp']
        if self.last_kline_timestamp != timestamp:
            self.market_cum_vol += self.last_kline_vol
            self.last_kline_size_of_market = self.last_kline_vol
            self.last_kline_timestamp = timestamp
        else:
            self.last_kline_vol = kline['metadata']['volume']

    def on_orderbook_ready(self, orderbook):
        """
        vwap main logic, driven by orderbook data
        :param orderbook: data format reference strategy_base
        """
        if (datetime.now() - str_to_datetime(self.task['start_time'])).total_seconds() < 0:
            # Time didn't come to start time
            return
        key = '|'.join([orderbook['exchange'], orderbook['symbol'], orderbook['contract_type'], 'orderbook'])
        self.market_data[key] = orderbook
        symbol = self.task['symbol'][0]
        if orderbook['exchange'] == self.task['exchange'] and orderbook['symbol'] == symbol:
            if self.last_order_time and (datetime.now() - self.last_order_time).total_seconds() < 3:
                # order interval is too close, wait
                return

            if len(self.pending_orders) > 0:
                # still has order not in response
                self.clear_timeout_pending_orders()
                logger.file(f"There have {len(self.pending_orders)} pending orders, wait")
                return

            # balance computed by order response limit to specific exchange;
            iceberg_balance = self.balance
            if not iceberg_balance:
                logger.warning("Didn't receive balance from pdt, please wait")
                # self.update_status(TaskStatus.WARNING.value, "Didn't receive balance")
                return
            base_currency, quote_currency = self.task['symbol'][1:3]

            price_precision = get_price_precision(self.task, self.task['exchange'], self.task['symbol'][0])
            amount_precision = get_amount_precision(self.task, self.task['exchange'], self.task['symbol'][0])
            contract_type = self.task['contract_type'] if 'contract_type' in self.task else 'spot'
            base_min_order_size, quote_min_order_size = get_min_order_size(self.task, self.task['exchange'],
                                                                           self.task['symbol'][0])
            bal_diff = get_total_balance(iceberg_balance, base_currency) - self.task['initial_balance'][base_currency]

            cum_exec_vol = bal_diff if self.task['direction'] == Direction.BUY.value else -bal_diff

            if self.task['order_mode']=='time_based':
                amount = order_size_cal(self.avg_vol_ref_mins, self.market_cum_vol, self.task['end_time'],
                                        self.task['total_size'], cum_exec_vol)
                logger.info(f"market_cum_vol: {self.market_cum_vol}; cum_exec_vol: {cum_exec_vol}; amount: {amount}")
            else:
                if (datetime.now() - self.last_order_time).total_seconds() < 60:
                    # execute every minute
                    return
                if (datetime.now() - get_datetime(self.last_kline_timestamp)).total_seconds() > 120:
                    # avoid outdated data, for exchange kline is not updated or updated by rest
                    self.last_kline_size_of_market = 0
                self.last_kline_size_of_customer = cum_exec_vol - self.customer_cum_vol_exclude_last_minute
                amount = cal_order_size_by_lkp(self.last_kline_size_of_market + self.cum_vol_not_used_of_market,
                                               self.last_kline_size_of_customer,
                                               self.task['fill_ratio'])
            if amount == 0:
                # order is over executed, wait
                # logger.file('size is zero')
                return

            post_only = True if self.task['trade_role'] == 'Maker' else False
            asks = orderbook['metadata']['asks']
            bids = orderbook['metadata']['bids']
            spread = asks[0][0] - bids[0][0]
            # self.inspect_order()
            # to do compute volume filter by orderbook
            if self.task['direction'] == Direction.SELL.value:
                price, size = price_filter_by_volume(bids, amount)
            else:
                price, size = price_filter_by_volume(asks, amount)

            if self.task['trade_role'] == 'Maker' and spread == price_precision:
                # only maker and no spread, set price to bid0 or ask0
                price = price

            price = format_price(price, price_precision)

            if not price or price <= 0:
                logger.warning(f"{self.task['symbol'][0]} price is not valid: {price}")
                return

            if len(self.active_orders) > 0:
                # only hold one order at most
                for ref_id, order_info in self.active_orders.items():
                    self.cancel_order(ref_id)
                    return

            if self.task['currency_type'] == CurrencyType.QUOTE.value:
                diff = get_total_balance(iceberg_balance, quote_currency) - self.task['initial_balance'][quote_currency]
                diff = -diff if self.task['direction'] == Direction.BUY.value else diff
                remain_amount = (self.task['total_size'] - diff) / price
            else:
                diff = get_total_balance(iceberg_balance, base_currency) - self.task['initial_balance'][base_currency]
                diff = diff if self.task['direction'] == Direction.BUY.value else -diff
                # compute_executed_volume(self.finished_orders)
                remain_amount = self.task['total_size'] - diff

            min_order_size = max(base_min_order_size, quote_min_order_size / price)
            logger.file(f'remain_amount: {remain_amount}; price: {price}; min_order_size: {min_order_size}')

            if abs(diff) >= self.task['total_size'] or remain_amount < min_order_size:
                self.on_vwap_finish()
                return

            if remain_amount - amount < min_order_size:
                # remain exceeds order size is less than min order size, compact this time,
                # to do set the order to market order
                amount = remain_amount

            amount = min(remain_amount, amount)

            amount = format_amount(amount, amount_precision)

            if not amount or amount <= min_order_size:
                # parameter is illegal
                logger.file(f'amount size is {amount}, lower than min_order_size {min_order_size}')
                # self.update_status(TaskStatus.WARNING.value, 'amount size is lower than min_order_size: {}'.format(min_order_size))
                if self.mkr_unused_tsp != self.last_kline_timestamp \
                        and (datetime.now() - get_datetime(self.last_kline_timestamp)).total_seconds() > 60:
                    self.cum_vol_not_used_of_market += self.last_kline_size_of_market
                    logger.info(f'cum_vol_not_used: {self.cum_vol_not_used_of_market}')
                    self.mkr_unused_tsp = self.last_kline_timestamp
                return
            if self.task['order_mode'] != 'time_based':
                logger.info(f'lksoc:{self.last_kline_size_of_customer}; lksom: {self.last_kline_size_of_market}')
            logger.file(f'remain_amount: {remain_amount}; price: {price}; min_order_size: {min_order_size}')
            self.customer_cum_vol_exclude_last_minute = cum_exec_vol
            self.cum_vol_not_used_of_market = 0
            if self.task['price_threshold'] is not None:
                if (self.task['direction'] == Direction.BUY.value and price < self.task['price_threshold']) \
                        or (self.task['direction'] == Direction.SELL.value and price > self.task['price_threshold']):
                    self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                    self.task['direction'], OrderType.LIMIT.value,
                                    self.task['account'], 'vwap', None, post_only)
                    self.last_order_time = datetime.now()
            else:
                # no price limit
                self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                self.task['direction'], OrderType.LIMIT.value,
                                self.task['account'], 'vwap', None, post_only)
                self.last_order_time = datetime.now()

    def on_response(self, response):
        super().on_response(response)

    def on_trade_ready(self, trade):
        pass

    def on_timer(self):
        """
        invoke on_timer of strategy_base; regular eat biding orders in aggressive mode
        """
        super().on_timer()

    def on_vwap_finish(self):
        """
        iceberg finish func; wait orders in pending_orders and active_orders executed
        """
        if len(self.pending_orders) > 0 or len(self.active_orders) > 0:
            time.sleep(3)
            logger.info('There still have orders not handled, sleep 3 seconds!')
            return
        logger.info('Vwap has finished!')
        self.status = TaskStatus.FINISHED.value
        self.status_msg = 'Vwap has finished'
        self.on_finish()

    def on_finish(self):
        super().on_finish()
