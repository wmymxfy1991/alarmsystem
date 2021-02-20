
import random
import time

from config.enums import *
from util.util import *
from util.logger import logger
from .strategy_base import StrategyBase


class Twap(StrategyBase):
    def __init__(self):
        super().__init__()
        self.twap_status = False
        self.base_currency = 0
        self.quote_currency = 0
        self.last_trigger = 0
        self.bid0 = 0
        self.ask0 = 0
        self.market_order_coefficient = 0.05
        self.order_interval = 60
        self.order_delay = (self.order_interval - 1) * 1000   # unit: ms

    def on_init(self, config, task, master_ptr):
        super().on_init(config, task, master_ptr)

    def on_book(self, quote):
        # 调用公用的行情处理
        super().on_book(quote)

    def on_orderbook_ready(self, quote):
        if quote['symbol'] == self.task['symbol'][0]:
            asks = quote['metadata']['asks']
            bids = quote['metadata']['bids']
            if 'orderbook_threshold' in self.task:
                self.bid0, self.ask0 = orderbook_price_filter(quote, self.task['orderbook_threshold'])
            else:
                self.bid0 = bids[0][0]
                self.ask0 = asks[0][0]

    def twap_start(self, task):
        # 1. check all params
        symbol = task["symbol"][0]
        base = task["symbol"][1]
        quote = task["symbol"][2]
        direction = task["direction"]
        currency_type = task["currency_type"]
        total_size = task["total_size"]

        st = int(time.mktime(time.strptime(task["start_time"], "%Y-%m-%d %H:%M:%S")))
        et = int(time.mktime(time.strptime(task["end_time"], "%Y-%m-%d %H:%M:%S")))
        ts_now = int(time.time())

        ini_balance = task["initial_balance"][base] if currency_type == CurrencyType.BASE.value else task["initial_balance"][quote]

        bid0 = self.bid0
        ask0 = self.ask0
        if not ask0 or not bid0:
            logger.error("ask0, bid0 value error!")
            return

        sym_describe = task["coin_config"][symbol]
        price_precision = sym_describe["price_precision"]
        amount_precision = sym_describe["size_precision"]
        min_size = max(sym_describe["base_min_order_size"], sym_describe["quote_min_order_size"] / bid0)
        min_size = math.ceil(min_size / amount_precision) * amount_precision

        offset = get_price_offset_from_prices(direction, ask0, bid0, price_precision, task['execution_mode'])
        # 2. check time and account status

        if task['exchange'] in BALANCE_BY_ORDER_RES_EX and BALANCE_BY_ORDER_RES_EX[task['exchange']]:
            balance_status = True
            base_currency = self.balance[base]['total']
            quote_currency = self.balance[quote]['total']
        else:
            if not self.balance:
                balance_status = False
                base_currency = 0
                quote_currency = 0
            else:
                balance_status = True
                base_currency = self.balance[base]['total'] if base in self.balance else 0
                quote_currency = self.balance[quote]['total'] if quote in self.balance else 0

        logger.info({
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "ask0": ask0,
            "bid0": bid0
        })

        if not self.twap_status:
            if ts_now >= st:
                if balance_status:
                    if direction == Direction.SELL.value:
                        if currency_type == CurrencyType.BASE.value:
                            if base_currency + amount_precision <= ini_balance - total_size:
                                logger.error("balance not enough!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.twap_status = True
                        else:
                            if base_currency <= (total_size - (quote_currency - ini_balance)) / bid0 / 2:
                                logger.error("balance not enough!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.twap_status = True
                    else:
                        if currency_type == CurrencyType.QUOTE.value:
                            if quote_currency <= ini_balance - total_size:
                                logger.error("balance not enough!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.twap_status = True
                        else:
                            if quote_currency <= (total_size - (base_currency - ini_balance)) * ask0 / 2:
                                logger.error("balance not enough!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.twap_status = True
                else:
                    self.twap_status = True
            else:
                logger.info("waiting for execution start:::")
                return

        # 3. whether twap finished or not

        if len(self.pending_orders) > 4:
            self.alarm(f'too many pending orders, {len(self.pending_orders)} pending orders:{self.pending_orders}',
                       AlarmCode.EXECUTE_ABNORMAL.value)
            self.clear_timeout_pending_orders()

        if self.active_orders:
            for ref_id in self.active_orders:
                self.cancel_order(ref_id)
            if len(self.active_orders) > 4:
                self.alarm(f'too many active orders, {len(self.active_orders)} active orders:{self.active_orders}',
                           AlarmCode.EXECUTE_ABNORMAL.value)
                return

        if self.twap_status:
            if balance_status:
                if direction == Direction.SELL.value:
                    if currency_type == CurrencyType.BASE.value:
                        if base_currency < ini_balance - total_size + min_size or base_currency < min_size:
                            self.twap_on_finish()
                    else:
                        if quote_currency > ini_balance + total_size or base_currency < min_size:
                            self.twap_on_finish()
                else:
                    if currency_type == CurrencyType.QUOTE.value:
                        if quote_currency < ini_balance - total_size + min_size * bid0 or quote_currency < min_size * bid0:
                            self.twap_on_finish()
                    else:
                        if base_currency > ini_balance + total_size - amount_precision or quote_currency < min_size * bid0:
                            self.twap_on_finish()
        else:
            return

        # 4. if twap not finish yet, define trade details

        should_trade = total_size * ((ts_now - st) / (et - st))
        should_trade = total_size if should_trade >= total_size else should_trade
        single_amount = total_size / (et - st) * self.order_interval

        if balance_status:
            if direction == Direction.SELL.value:
                price = ask0 * (1 + offset)
                market_price = bid0
                if currency_type == CurrencyType.BASE.value:
                    balance_diff = ini_balance - base_currency
                    amount = 0 if balance_diff >= should_trade else single_amount
                    market_amount = max(should_trade - balance_diff - single_amount, 0)

                    if total_size - balance_diff <= 2 * max(single_amount, min_size):
                        amount = 0
                        market_price = bid0 * (1 - self.market_order_coefficient)
                        market_amount = total_size - balance_diff
                else:
                    balance_diff = quote_currency - ini_balance
                    amount = 0 if balance_diff >= should_trade else single_amount / price
                    market_amount = max((should_trade - balance_diff - single_amount) / market_price, 0)

                    if total_size - balance_diff <= 2 * max(single_amount, 2 * min_size * bid0):
                        amount = 0
                        market_price = bid0 * (1 - self.market_order_coefficient)
                        market_amount = (total_size - balance_diff) / market_price
            else:
                price = bid0 * (1 + offset)
                market_price = ask0
                if currency_type == CurrencyType.QUOTE.value:
                    balance_diff = ini_balance - quote_currency
                    amount = 0 if balance_diff >= should_trade else single_amount / price
                    market_amount = max((should_trade - balance_diff - single_amount) / market_price, 0)

                    if total_size - balance_diff <= 2 * max(single_amount, 2 * min_size * ask0):
                        amount = 0
                        market_price = ask0
                        market_amount = math.floor((total_size - balance_diff) / market_price / amount_precision) * amount_precision
                else:
                    balance_diff = base_currency - ini_balance
                    amount = 0 if balance_diff >= should_trade else single_amount
                    market_amount = max(should_trade - balance_diff - single_amount + amount_precision, 0)

                    if total_size - balance_diff <= 2 * max(single_amount, min_size):
                        amount = 0
                        market_price = ask0 * (1 + self.market_order_coefficient)
                        market_amount = total_size - balance_diff
        else:
            balance_diff = 0
            if ts_now <= et:
                if direction == Direction.SELL.value:
                    price = ask0 * (1 + offset)
                    amount = 0
                    market_price = bid0
                    market_amount = single_amount
                    if currency_type == CurrencyType.QUOTE.value:
                        market_amount = single_amount / market_price
                else:
                    price = bid0 * (1 + offset)
                    amount = 0
                    market_price = ask0
                    market_amount = single_amount
                    if currency_type == CurrencyType.QUOTE.value:
                        market_amount = single_amount / market_price
            else:
                return

        # 5. define order details and send_order
        logger.info({
            "initial balance": ini_balance,
            "balance_diff": balance_diff,
            "should_trade": should_trade,
        })
        price_threshold = False if 'price_threshold' not in task or task["price_threshold"] is None else task["price_threshold"]
        self.send_formated_order(symbol, direction, price, amount, price_precision, amount_precision, min_size, price_threshold)
        self.send_formated_order(symbol, direction, market_price, market_amount, price_precision, amount_precision, min_size, price_threshold)
        return

    def on_response(self, response):
        # print('on_response---------------', response)
        super().on_response(response)
        pass

    def on_timer(self):
        super().on_timer()
        if 'fixed_interval' in self.task and 'random_interval' in self.task:
            time_inverval = (self.task["fixed_interval"] + self.task["random_interval"] * random.random()) / 1000
        else:
            time_inverval = 60
        if time.time() - self.last_trigger >= time_inverval:
            self.last_trigger = time.time()
            if self.status != TaskStatus.PAUSED.value:
                self.twap_start(self.task)
        else:
            pass

    def on_finish(self):
        super().on_finish()

    def twap_on_finish(self):
        self.twap_status = False
        self.update_status(TaskStatus.FINISHED.value, 'TWAP has finished!')
        self.on_finish()

    def send_formated_order(self, symbol, direction, price, amount, price_precision, amount_precision, min_size, price_threshold):
        price = format_price(price, price_precision)
        amount = format_amount(amount, amount_precision)
        amount = amount_adjust(amount, amount_precision, min_size)
        if price_threshold:
            if direction == Direction.SELL.value:
                amount = 0 if price < price_threshold else amount
            else:
                amount = 0 if price > price_threshold else amount
        if amount:
            self.send_order(self.task["exchange"], symbol, 'spot', price, amount, direction, OrderType.LIMIT.value, self.task["account"],
                            'Twap', self.order_delay)