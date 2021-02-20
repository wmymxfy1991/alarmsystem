# encoding: utf-8
#
import random
from datetime import datetime

from config.enums import *
from strategy.strategy_base import StrategyBase
from util.logger import logger
from util.util import *


class Iceberg(StrategyBase):
    def __init__(self):
        super().__init__()
        self.trading = True
        self.market_data = {}  # latest market data
        self.trade_data = {}  # trade data store
        self.last_order_time = None  # latest send order time
        self.aggressive_order_time = datetime.now()  # used to execute aggressive mode of iceberg in specific interval
        self.aggressive_price_overflow = 0.01  # price more or less than price threshold in amplitude
        self.aggressive_interval = 120  # set aggressive execute interval to 2 minutes
        self.order_interval = 5  # order interval is at least 5s
        self.trade_size_in_last_minute = 0  # trade size in last minute
        self.trade_list = []  # trade list in last minute

    def on_init(self, config, task, master_ptr):
        """
        reference to strategy_base
        """
        super().on_init(config, task, master_ptr)

    def on_book(self, market_data):
        # 调用公用的行情处理
        super().on_book(market_data)

    def on_orderbook_ready(self, orderbook):
        """
        iceberg main logic, driven by orderbook data
        :param orderbook: data format reference strategy_base
        """
        if (datetime.now() - str_to_datetime(self.task['start_time'])).total_seconds() < 0:
            # Time didn't come to start time
            return
        key = '|'.join([orderbook['exchange'], orderbook['symbol'], orderbook['contract_type'], 'orderbook'])
        self.market_data[key] = orderbook
        if self.last_order_time and (datetime.now() - self.last_order_time).total_seconds() < self.order_interval:
            # order interval is too close, wait
            return

        if len(self.pending_orders) > 0:
            # still has order not in response
            self.clear_timeout_pending_orders()
            logger.file(f"There have {len(self.pending_orders)} pending orders, wait")
            return
        
        # balance computed by order response limit to specific exchange;
        # TO DO: migrate to strategy_base
        iceberg_balance = self.balance
        if not iceberg_balance:
            logger.warning("Didn't receive balance from pdt, please wait")
            # self.update_status(TaskStatus.WARNING.value, "Didn't receive balance")
            return
            
        symbol = self.task['symbol'][0]
   
        price_precision = get_price_precision(self.task, self.task['exchange'], self.task['symbol'][0])
        amount_precision = get_amount_precision(self.task, self.task['exchange'], self.task['symbol'][0])
        contract_type = self.task['contract_type'] if 'contract_type' in self.task else 'spot'
        base_min_order_size, quote_min_order_size = get_min_order_size(self.task, self.task['exchange'], self.task['symbol'][0])
        post_only = True if self.task['trade_role'] == 'Maker' else False
        ob_s = 0
        tr_s = sum([_[4] for _ in self.trade_list])
        if orderbook['exchange'] == self.task['exchange'] and orderbook['symbol'] == symbol:
            asks = orderbook['metadata']['asks']
            bids = orderbook['metadata']['bids']
            spread = asks[0][0] - bids[0][0]
            # self.inspect_order()
            # to do compute volume filter by orderbook
            volume_filter = get_volume_filter(self.task)
            if self.task['direction'] == Direction.SELL.value:
                ob_s = cal_ob_avg_size(asks, 5)
                price, size = price_filter_by_volume(asks, volume_filter)
            else:
                price, size = price_filter_by_volume(bids, volume_filter)
                ob_s = cal_ob_avg_size(bids, 5)

            if self.task['trade_role'] == 'Maker' and spread == price_precision:
                # only maker and no spread, set price to bid0 or ask0
                price = price
            else:
                price = price + price_precision if self.task['direction'] == Direction.BUY.value else price - price_precision
            price = format_price(price, price_precision)
            # normally iceberg only have one active order, detect whether our order is at best place
            if len(self.active_orders) > 0:
                for ref_id, order_info in self.active_orders.items():
                    if order_info['price'] == asks[0][0] or order_info['price'] == bids[0][0]:
                        # order price is at best
                        return
                    else:
                        self.cancel_order(ref_id)
                        return

            # amount = random.random() * self.task['iceberg_amount'][1] + self.task['iceberg_amount'][0]
            base_currency, quote_currency = self.task['symbol'][1:3]
            if self.task['currency_type'] == CurrencyType.QUOTE.value:
                diff = get_total_balance(iceberg_balance, quote_currency) - self.task['initial_balance'][quote_currency]
                diff = -diff if self.task['direction'] == Direction.BUY.value else diff
                residual_amount = (self.task['total_size'] - diff) / price
            else:
                diff = get_total_balance(iceberg_balance, base_currency) - self.task['initial_balance'][base_currency]
                diff = diff if self.task['direction'] == Direction.BUY.value else -diff
                # compute_executed_volume(self.finished_orders)
                residual_amount = self.task['total_size'] - diff

            if not price or price <= 0:
                logger.warning(f"{self.task['symbol'][0]} price is not valid: {price}")
                return
            min_order_size = max(base_min_order_size, quote_min_order_size / price)
            logger.info(f'diff:{diff}; residual_amount: {residual_amount}; price: {price}; min_order_size: {min_order_size}')
            if abs(diff) >= self.task['total_size'] or residual_amount < min_order_size:
                self.on_iceberg_finish()
                return
            amount = cal_order_size_by_ob_tr(ob_s, tr_s, min_order_size, MAX_SIZE_BY_QUOTE[quote_currency]/price)
            logger.info(f"ob_s{ob_s}; tr_s: {tr_s}; min_order_size: {min_order_size}; max_order_size: {MAX_SIZE_BY_QUOTE[quote_currency]/price}; amount: {amount}")
            if residual_amount - amount < min_order_size:
                # residual exceeds order size is less than min order size, compact this time,
                # to do set the order to market order
                amount = residual_amount
            amount = min(residual_amount, amount)

            amount = format_amount(amount, amount_precision)
            
            if not amount or amount <= min_order_size:
                # parameter is illegal
                logger.warning(f'amount size is {amount}, lower than min_order_size {min_order_size}')
                # self.update_status(TaskStatus.WARNING.value, 'amount size is lower than min_order_size: {}'.format(min_order_size))
                return
            if self.task['price_threshold'] is not None:
                if (self.task['direction'] == Direction.BUY.value and price < self.task['price_threshold']) \
                        or (self.task['direction'] == Direction.SELL.value and price > self.task['price_threshold']):
                    self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                    self.task['direction'], OrderType.LIMIT.value,
                                    self.task['account'], 'Iceberg', None, post_only)
                    self.last_order_time = datetime.now()
            else:
                # no price limit
                self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                self.task['direction'], OrderType.LIMIT.value,
                                self.task['account'], 'Iceberg', None, post_only)

    def on_trade_ready(self, trade):
        if trade['exchange'] == self.task['exchange'] and trade['symbol'] == self.task['symbol'][0]:
            if len(trade['metadata']) > 0:
                for _ in trade['metadata']:
                    if (datetime.now() - str_to_datetime(_[1])).total_seconds() <= 60:
                        self.trade_list.append(_)

    def trade_check(self):
        if len(self.trade_list) > 0:
            for trade in self.trade_list:
                if (datetime.now() - str_to_datetime(trade[1])).total_seconds() > 60:
                    self.trade_list.remove(trade)


    def on_response(self, response):
        super().on_response(response)

    def on_timer(self):
        """
        invoke on_timer of strategy_base; regular eat biding orders in aggressive mode
        """
        super().on_timer()
        self.trade_check()

        if self.task['execution_mode'] == 'Aggressive':
            if (datetime.now() - str_to_datetime(self.task['start_time'])).total_seconds() < 0:
                # Time didn't come to start time
                return

            #  stop send_order logic if in paused status
            if self.status == TaskStatus.PAUSED.value:
                return

            if len(self.pending_orders) > 0:
                # still has order not in response
                self.clear_timeout_pending_orders()
                logger.file(f"There have {len(self.pending_orders)} pending orders, wait")
                return

            if len(self.active_orders) > 1:
                logger.file(f"There have {len(self.active_orders)} active orders, wait")
                return

            if (datetime.now() - self.aggressive_order_time).total_seconds() > self.aggressive_interval:
                # balance computed by order response limit to specific exchange;
                iceberg_balance = self.balance
                if not iceberg_balance:
                    logger.warning("Didn't receive balance from pdt, please wait")
                    # self.update_status(TaskStatus.WARNING.value, "Didn't receive balance")
                    return
                symbol = self.task['symbol'][0]
                contract_type = self.task['contract_type'] if 'contract_type' in self.task else 'spot'
                price_precision = get_price_precision(self.task, self.task['exchange'], symbol)
                amount_precision = get_amount_precision(self.task, self.task['exchange'], symbol)

                key = '|'.join([self.task['exchange'], symbol, contract_type, 'orderbook'])
                if key not in self.market_data:
                    logger.warning(f"Didn't receive market data:{key}")
                    return
                orderbook = self.market_data[key]
                if self.task['direction'] == Direction.SELL.value:
                    price, amount = orderbook['metadata']['bids'][0]
                else:
                    price, amount = orderbook['metadata']['asks'][0]
                price = format_price(price, price_precision)
                if not price or price <= 0:
                    return

                base_min_order_size, quote_min_order_size = get_min_order_size(self.task, self.task['exchange'],
                                                                               self.task['symbol'][0])
                min_order_size = max(base_min_order_size, quote_min_order_size / price)
                if amount < min_order_size:
                    logger.warning("Order amount didn't meet min order size")
                    amount = min_order_size

                base_currency, quote_currency = get_base_quote_currency_name(self.task, self.task['exchange'], 'symbol')
                if self.task['currency_type'] == CurrencyType.QUOTE.value:
                    diff = get_total_balance(iceberg_balance, quote_currency) - self.task['initial_balance'][quote_currency]
                    diff = -diff if self.task['direction'] == Direction.BUY.value else diff
                    residual_amount = (self.task['total_size'] - diff) / price
                else:
                    diff = get_total_balance(iceberg_balance, base_currency) - self.task['initial_balance'][base_currency]
                    diff = diff if self.task['direction'] == Direction.BUY.value else -diff
                    residual_amount = self.task['total_size'] - diff
                if abs(diff) >= self.task['total_size'] or residual_amount < min_order_size:
                    self.on_iceberg_finish()
                    return
                amount = min(residual_amount, amount)
                amount = format_amount(amount, amount_precision)
                if self.task['price_threshold'] is not None:
                    if (self.task['direction'] == Direction.BUY.value and price < self.task['price_threshold']) \
                            or (self.task['direction'] == Direction.SELL.value and price > self.task['price_threshold']):
                        self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                        self.task['direction'], OrderType.LIMIT.value,
                                        self.task['account'], 'Iceberg')
                else:
                    # no price limit set
                    self.send_order(self.task['exchange'], symbol, contract_type, price, amount,
                                    self.task['direction'], OrderType.LIMIT.value,
                                    self.task['account'], 'Iceberg')

    def on_iceberg_finish(self):
        """
        iceberg finish func; wait orders in pending_orders and active_orders executed
        """
        if len(self.pending_orders) > 0 or len(self.active_orders) > 0:
            time.sleep(3)
            logger.info('There still have orders not handled, sleep 3 seconds!')
            return
        logger.info('Iceberg has finished!')
        self.status = TaskStatus.FINISHED.value
        self.status_msg = 'Iceberg has finished'
        self.on_finish()

    def on_finish(self):
        super().on_finish()
