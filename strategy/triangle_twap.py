import random
import time

from config.enums import *
from util.util import *
from util.logger import logger
from .strategy_base import StrategyBase


class TriangleTwap(StrategyBase):
    def __init__(self):
        super().__init__()
        self.triangle_twap_status = False
        self.base_currency = 0
        self.median_currency = 0
        self.quote_currency = 0
        self.last_trigger = 0
        self.anchor_bid0 = 0
        self.anchor_ask0 = 0
        self.median_bid0 = 0
        self.median_ask0 = 0
        self.market_order_coefficient = 0.05
        self.order_interval = 60
        self.order_delay = (self.order_interval - 1) * 1000   # unit: ms

    def on_init(self, config, task, master_ptr):
        super().on_init(config, task, master_ptr)

    def on_book(self, quote):
        # 调用公用的行情处理
        super().on_book(quote)

    def on_orderbook_ready(self, quote):
        asks = quote['metadata']['asks']
        bids = quote['metadata']['bids']
        if quote['symbol'] == self.task['median'][0]:
            self.median_bid0 = bids[0][0]
            self.median_ask0 = asks[0][0]
        elif quote['symbol'] == self.task['anchor'][0]:
            self.anchor_bid0 = bids[0][0]
            self.anchor_ask0 = asks[0][0]

    def triangle_twap_start(self, task):

        # 1. check and define all params

        median = task["median"][0]
        anchor = task["anchor"][0]
        base = task["symbol"][1]
        quote = task["symbol"][2]
        direction = task["direction"]
        currency_type = task["currency_type"]
        total_size = task["total_size"]

        st = int(time.mktime(time.strptime(task["start_time"], "%Y-%m-%d %H:%M:%S")))
        et = int(time.mktime(time.strptime(task["end_time"], "%Y-%m-%d %H:%M:%S")))
        ts_now = int(time.time())
        mid_coin = get_mid_coin_from_triangle_pair(task["symbol"], task["median"], task["anchor"])

        if not self.median_bid0 or not self.median_ask0 or not self.anchor_bid0 or not self.anchor_ask0 or not mid_coin:
            return

        ini_balance = task["initial_balance"][base] if currency_type == CurrencyType.BASE.value else task["initial_balance"][quote]
        mid_ini_balance = task["initial_balance"][mid_coin]

        # define directions for median and anchor
        if direction == Direction.BUY.value:
            symbol_1 = anchor
            symbol_2 = median

            bid0_1 = self.anchor_bid0
            ask0_1 = self.anchor_ask0
            bid0_2 = self.median_bid0
            ask0_2 = self.median_ask0

            direction_2 = Direction.BUY.value
            direction_1 = Direction.BUY.value if task['anchor'][1] == mid_coin else Direction.SELL.value

            # define: median and anchor offset
            offset_1 = 0
            offset_2 = get_price_offset_from_prices(direction_2, ask0_2, bid0_2, task["coin_config"][median]["price_precision"], task['execution_mode'])

            # define: precision, decimal, min trade size
            anchor_describe = task["coin_config"][anchor]
            median_describe = task["coin_config"][median]
            # anchor
            price_pre_1 = anchor_describe["price_precision"]
            amount_pre_1 = anchor_describe["size_precision"]
            price_dec_1 = get_decimal_from_precision(price_pre_1)
            amount_dec_1 = get_decimal_from_precision(amount_pre_1)

            min_size_1 = max(anchor_describe["base_min_order_size"], anchor_describe["quote_min_order_size"] / bid0_1)
            min_size_1 = math.ceil(min_size_1 / amount_pre_1) * amount_pre_1

            # median
            price_pre_2 = median_describe["price_precision"]
            amount_pre_2 = median_describe["size_precision"]
            price_dec_2 = get_decimal_from_precision(price_pre_2)
            amount_dec_2 = get_decimal_from_precision(amount_pre_2)

            min_size_2 = max(median_describe["base_min_order_size"], median_describe["quote_min_order_size"] / bid0_2)
            min_size_2 = math.ceil(min_size_2 / amount_pre_2) * amount_pre_2

            if 'price_threshold' in task and task['price_threshold'] is not None:
                price_threshold_2 = task['price_threshold']
                price_threshold_1 = False
            elif 'anchor_price' in task and task['anchor_price'] is not None:
                price_threshold_1 = task['anchor_price'] / ask0_2 if direction_1 == Direction.BUY.value else ask0_2 / task['anchor_price']
                price_threshold_2 = task['anchor_price'] / ask0_1 if direction_1 == Direction.BUY.value else task['anchor_price'] * bid0_1
            else:
                price_threshold_1 = False
                price_threshold_2 = False
        else:
            symbol_1 = median
            symbol_2 = anchor

            bid0_1 = self.median_bid0
            ask0_1 = self.median_ask0

            bid0_2 = self.anchor_bid0
            ask0_2 = self.anchor_ask0

            direction_1 = Direction.SELL.value
            direction_2 = Direction.SELL.value if task['anchor'][1] == mid_coin else Direction.BUY.value

            # define: median and anchor offset
            offset_1 = get_price_offset_from_prices(direction_1, ask0_1, bid0_1, task["coin_config"][median]["price_precision"], task['execution_mode'])
            offset_2 = 0

            # define: precision, decimal, min trade size
            anchor_describe = task["coin_config"][anchor]
            median_describe = task["coin_config"][median]
            # median
            price_pre_1 = median_describe["price_precision"]
            amount_pre_1 = median_describe["size_precision"]

            price_dec_1 = get_decimal_from_precision(price_pre_1)
            amount_dec_1 = get_decimal_from_precision(amount_pre_1)

            min_size_1 = max(median_describe["base_min_order_size"], median_describe["quote_min_order_size"] / bid0_1)
            min_size_1 = math.ceil(min_size_1 / amount_pre_1) * amount_pre_1

            # anchor
            price_pre_2 = anchor_describe["price_precision"]
            amount_pre_2 = anchor_describe["size_precision"]

            price_dec_2 = get_decimal_from_precision(price_pre_2)
            amount_dec_2 = get_decimal_from_precision(amount_pre_2)

            min_size_2 = max(anchor_describe["base_min_order_size"], anchor_describe["quote_min_order_size"] / bid0_2)
            min_size_2 = math.ceil(min_size_2 / amount_pre_2) * amount_pre_2

            if 'price_threshold' in task and task['price_threshold'] is not None:
                price_threshold_2 = False
                price_threshold_1 = task['price_threshold']
            elif 'anchor_price' in task and task['anchor_price'] is not None:
                price_threshold_1 = task['anchor_price'] / bid0_2 if direction_2 == Direction.SELL.value else task['anchor_price'] * ask0_2
                price_threshold_2 = task['anchor_price'] / bid0_1 if direction_2 == Direction.SELL.value else bid0_1 / task['anchor_price']
            else:
                price_threshold_1 = False
                price_threshold_2 = False

        if not self.triangle_twap_status:
            logger.info({
                "symbol_1": symbol_1,
                "direction_1": direction_1,
                "bid0_1|ask0_1": [bid0_1, ask0_1],
                "offset_1": offset_1,
                "price_precision_1": price_pre_1,
                "price_decimal_1": price_dec_1,
                "amount_precision_1": amount_pre_1,
                "amount_decimal_1": amount_dec_1,
                "min_size_1": min_size_1,
                "price_threshold_1": price_threshold_1,

                "symbol_2": symbol_2,
                "direction_2": direction_2,
                "bid0_2|ask0_2": [bid0_2, ask0_2],
                "offset_2": offset_2,
                "price_precision_2": price_pre_2,
                "price_decimal_2": price_dec_2,
                "amount_precision_2": amount_pre_2,
                "amount_decimal_2": amount_dec_2,
                "min_size_2": min_size_2,
                "price_threshold_2": price_threshold_2
            })

        # balances
        if task['exchange'] in BALANCE_BY_ORDER_RES_EX and BALANCE_BY_ORDER_RES_EX[task['exchange']]:
            balance_status = True
            base_currency = self.balance[base]['total']
            quote_currency = self.balance[quote]['total']
            mid_currency = self.balance[mid_coin]['total']
        else:
            if not self.balance:
                return
            else:
                balance_status = True
                base_currency = self.balance[base]['total'] if base in self.balance else 0
                quote_currency = self.balance[quote]['total'] if quote in self.balance else 0
                mid_currency = self.balance[mid_coin]['total'] if mid_coin in self.balance else 0

        logger.info({
            "ini_base": task["initial_balance"][base],
            "ini_quote": task["initial_balance"][quote],
            "ini_middle": mid_ini_balance,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "mid_currency": mid_currency
        })

        # 2. check time and account status

        if not self.triangle_twap_status:
            if ts_now >= st:
                if balance_status:
                    if direction == Direction.SELL.value:
                        if currency_type == CurrencyType.BASE.value:
                            if base_currency <= ini_balance - total_size:
                                logger.error("balance not enough!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.triangle_twap_status = True
                        else:
                            logger.info("execution start:::")
                            self.triangle_twap_status = True
                    else:
                        if currency_type == CurrencyType.QUOTE.value:
                            if quote_currency >= ini_balance + total_size:
                                logger.error("balance wrong!")
                                return
                            else:
                                logger.info("execution start:::")
                                self.triangle_twap_status = True
                        else:
                            logger.info("execution start:::")
                            self.triangle_twap_status = True
                else:
                    return
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

        if self.triangle_twap_status:
            if direction == Direction.SELL.value:
                # default: Base
                if currency_type == CurrencyType.BASE.value:
                    balance_now = base_currency
                    end_balance = ini_balance - total_size

                    # exit if reach target size or can't send order
                    # if main trade end
                    if balance_now < end_balance + min_size_1 or base_currency < min_size_1:
                        if direction_2 == Direction.SELL.value and mid_currency - mid_ini_balance < min_size_2:
                                self.triangle_twap_on_finish()
                        if direction_2 == Direction.BUY.value and mid_currency - mid_ini_balance < min_size_2 * ask0_2:
                                self.triangle_twap_on_finish()
                else:
                    balance_now = quote_currency
                    end_balance = ini_balance + total_size

                    # exit if reach target size or balance not enough
                    if balance_now >= end_balance:
                        self.triangle_twap_on_finish()
                    if direction_2 == Direction.SELL.value and base_currency < min_size_1 and mid_currency < min_size_2:
                        self.triangle_twap_on_finish()
                    if direction_2 == Direction.BUY.value and base_currency < min_size_1 and mid_currency < min_size_2 * ask0_2:
                        self.triangle_twap_on_finish()
            else:
                # default Quote
                if currency_type == CurrencyType.QUOTE.value:
                    balance_now = quote_currency
                    end_balance = ini_balance - total_size

                    # exit if reach target size or can't send order
                    if direction_1 == Direction.SELL.value:
                        # if main trade end
                        if balance_now < end_balance + min_size_1 or quote_currency < min_size_1:
                            if mid_currency - mid_ini_balance < min_size_1:
                                self.triangle_twap_on_finish()
                    else:
                        # if main trade end
                        if balance_now < end_balance + min_size_1 * bid0_1 or quote_currency < min_size_1 * bid0_1:
                            if mid_currency - mid_ini_balance < min_size_1 * ask0_1:
                                self.triangle_twap_on_finish()
                else:
                    balance_now = base_currency
                    end_balance = ini_balance + total_size

                    # exit if reach target size or balance not enough
                    # if main trade end
                    if balance_now >= end_balance:
                        self.triangle_twap_on_finish()
                    if direction_1 == Direction.SELL.value and quote_currency < min_size_1 and mid_currency < min_size_2 * ask0_2:
                        self.triangle_twap_on_finish()
                    if direction_1 == Direction.BUY.value and quote_currency < min_size_1 * ask0_1 and mid_currency < min_size_2 * ask0_2:
                        self.triangle_twap_on_finish()
        else:
            return

        # 4. if twap not finish yet, define trade details

        should_trade = total_size * ((ts_now - st) / (et - st))
        should_trade = total_size if should_trade >= total_size else should_trade
        single_amount = total_size / (et - st) * self.order_interval

        if direction == Direction.SELL.value:
            price_1 = ask0_1 * (1 + offset_1)
            market_price_1 = bid0_1
            if currency_type == CurrencyType.BASE.value:
                balance_diff = ini_balance - balance_now
                amount_1 = 0 if balance_diff >= should_trade - amount_pre_1 else single_amount
                market_amount_1 = max(should_trade - balance_diff - single_amount, 0)

                # if execution going to end
                if total_size - balance_diff <= 2 * max(single_amount, min_size_1):
                    amount_1 = 0
                    market_price_1 = bid0_1 * (1 - self.market_order_coefficient)
                    market_amount_1 = total_size - balance_diff
                # symbol_2 trade details
                if direction_2 == Direction.SELL.value:
                    price_2 = bid0_2
                    amount_2 = mid_currency - mid_ini_balance
                else:
                    price_2 = ask0_2
                    amount_2 = math.floor((mid_currency - mid_ini_balance) / amount_pre_2) * amount_pre_2 / ask0_2

                market_price_2 = bid0_2
                market_amount_2 = 0
            else:
                balance_diff = balance_now - ini_balance
                single_mid_amount = single_amount / bid0_2 if direction_2 == Direction.SELL.value else single_amount * ask0_2
                amount_1 = 0 if balance_diff >= should_trade else single_mid_amount / bid0_1

                if direction_2 == Direction.SELL.value:
                    market_mid_amount = (should_trade - balance_diff - single_amount) / bid0_2
                else:
                    market_mid_amount = (should_trade - balance_diff - single_amount) * ask0_2
                market_amount_1 = max(market_mid_amount / bid0_1, 0)

                # if execution going to end
                if total_size - balance_diff <= 2 * max(single_amount, min_size_1 * bid0_1):
                    amount_1 = 0
                    if direction_2 == Direction.SELL.value:
                        market_mid_amount = (total_size - balance_diff) / bid0_2
                    else:
                        market_mid_amount = (total_size - balance_diff) * ask0_2
                    market_amount_1 = market_mid_amount / bid0_1

                # symbol_2 trade asap
                price_2 = bid0_2
                amount_2 = 0
                if direction_2 == Direction.SELL.value:
                    market_price_2 = bid0_2
                    market_amount_2 = mid_currency - mid_ini_balance
                else:
                    market_price_2 = ask0_2
                    market_amount_2 = (mid_currency - mid_ini_balance) / ask0_2
        else:
            price_2 = bid0_2 * (1 + offset_2)
            market_price_2 = ask0_2

            if currency_type == CurrencyType.QUOTE.value:
                balance_diff = ini_balance - quote_currency

                if direction_1 == Direction.SELL.value:
                    price_1 = bid0_1
                    amount_1 = should_trade - (ini_balance - balance_now)
                    market_price_1 = bid0_1
                    market_amount_1 = 0

                    amount_2 = (single_amount * bid0_1) / ask0_2
                else:
                    price_1 = ask0_1
                    amount_1 = (should_trade - (ini_balance - balance_now)) / ask0_1
                    market_price_1 = ask0_1
                    market_amount_1 = 0

                    amount_2 = (single_amount / ask0_1) / ask0_2
                market_amount_2 = (mid_currency - mid_ini_balance) / ask0_2

                # if execution going to end
                if total_size - balance_diff <= 2 * max(single_amount, min_size_1 * bid0_1):
                    if direction_1 == Direction.SELL.value:
                        amount_1 = total_size - balance_diff
                    else:
                        amount_1 = math.floor((total_size - balance_diff) / amount_pre_1) * amount_pre_1 / ask0_1
                    amount_2 = 0
                    market_amount_2 = (mid_currency - mid_ini_balance) / ask0_2
            else:
                balance_diff = ini_balance - quote_currency
                # price_1, market_price_1, amount_1, market_amount_1
                single_mid_amount = single_amount * ask0_2
                amount_1 = single_mid_amount / bid0_1 if direction_1 == Direction.SELL.value else single_mid_amount
                if direction_1 == Direction.SELL.value:
                    price_1 = bid0_1
                    market_price_1 = bid0_1
                    market_amount_1 = (should_trade - balance_diff) * ask0_2 / bid0_1 - amount_1
                else:
                    price_1 = ask0_1
                    market_price_1 = ask0_1
                    market_amount_1 = (should_trade - balance_diff) * ask0_2 - amount_1
                # amount_2, market_amount_2
                amount_2 = single_amount
                market_amount_2 = (mid_currency - mid_ini_balance) / price_2 - amount_2

                # if execution going to end
                if total_size - balance_diff <= 2 * max(single_amount, min_size_1):
                    if direction_1 == Direction.SELL.value:
                        market_amount_1 = (total_size - balance_diff) * ask0_2 / bid0_1
                    else:
                        market_amount_1 = (total_size - balance_diff) * ask0_2
                    amount_1 = 0
                    amount_2 = 0
                    market_amount_2 = total_size - balance_diff

        # 5. define order details and send_order
        logger.info({
            "initial balance": ini_balance,
            "balance_now": balance_now,
            "balance_diff": balance_diff,
            "should_trade": should_trade,
            "end_balance": end_balance
        })
        self.send_formated_order(symbol_1, direction_1, price_1, amount_1, price_pre_1, amount_pre_1, min_size_1, price_threshold_1)
        self.send_formated_order(symbol_1, direction_1, market_price_1, market_amount_1, price_pre_1, amount_pre_1, min_size_1, price_threshold_1)
        self.send_formated_order(symbol_2, direction_2, price_2, amount_2, price_pre_2, amount_pre_2, min_size_2, price_threshold_2, mid_coin)
        self.send_formated_order(symbol_2, direction_2, market_price_2, market_amount_2, price_pre_2, amount_pre_2, min_size_2, price_threshold_2, mid_coin)
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
                self.triangle_twap_start(self.task)
        else:
            pass

    def on_finish(self):
        super().on_finish()

    def triangle_twap_on_finish(self):
        self.triangle_twap_status = False
        self.update_status(TaskStatus.FINISHED.value, 'Triangle TWAP has finished!')
        self.on_finish()

    def send_formated_order(self, symbol, direction, price, amount, price_precision, amount_precision, min_size, price_threshold=False, mid_coin=False):
        price = format_price(price, price_precision)
        amount = format_amount(amount, amount_precision)
        if mid_coin:
            amount = amount_adjust(amount, amount_precision, min_size) if amount > min_size else 0
        else:
            amount = amount_adjust(amount, amount_precision, min_size)
        if price_threshold:
            if direction == Direction.SELL.value:
                amount = 0 if price < price_threshold else amount
            else:
                amount = 0 if price > price_threshold else amount
        if amount:
            self.send_order(self.task["exchange"], symbol, 'spot', price, amount, direction, OrderType.LIMIT.value, self.task["account"],
                            'TriangleTwap', self.order_delay)