# encoding: utf-8

import random
from datetime import datetime

from config.enums import *
from strategy.strategy_base import StrategyBase
from util.util import *
from util.logger import *


class TriangleIceberg(StrategyBase):
    def __init__(self):
        super().__init__()
        self.trading = True
        self.market_data = {}  # latest market data snapshot
        self.last_order_time = None  # last order time
        self.aggressive_order_time = datetime.now()  # aggressive mode send_order time
        self.aggressive_price_overflow = 0.01  # price more or less than price threshold in amplitude
        self.aggressive_interval = 120  # 2minutes
        self.order_interval = 5  # order interval is at least 5s
        self.last_m_price = 0  # last median price
        self.last_a_price = 0  # last anchor price
        self.median_stop_status = False  # median symbol stop flag, default to False
        self.anchor_stop_status = False  # anchor symbol stop flag, default to False
        self.anchor_trading = False  # whether start to do anchor trade, default to False
        self.trade_size_in_last_minute = 0  # trade size in last minute
        self.trade_list = []  # trade list in last minute
        self.last_m_amount = 0  # last median symbol order size

    def on_trade_ready(self, trade):
        if trade['exchange'] == self.task['exchange'] and trade['symbol'] == self.task['median'][0]:
            if len(trade['metadata']) > 0:
                for _ in trade['metadata']:
                    if (datetime.now() - str_to_datetime(_[1])).total_seconds() <= 60:
                        self.trade_list.append(_)

    def trade_check(self):
        if len(self.trade_list) > 0:
            for trade in self.trade_list:
                if (datetime.now() - str_to_datetime(trade[1])).total_seconds() > 60:
                    self.trade_list.remove(trade)

    def on_init(self, config, task, master_ptr):
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
        
        # balance computed by order response limit to specific exchange;
        t_iceberg_balance = self.balance
        if not t_iceberg_balance:
            logger.warning("Didn't receive balance, please wait")
            return

        key = '|'.join([orderbook['exchange'], orderbook['symbol'], orderbook['contract_type'], 'orderbook'])
        self.market_data[key] = orderbook

        if orderbook['exchange'] == self.task['exchange'] and orderbook['symbol'] == self.task['median'][0]:
            if self.last_order_time and (datetime.now() - self.last_order_time).total_seconds() < self.order_interval:
                # order interval is too close, wait
                return

            anchor_key = '|'.join([self.task['exchange'], self.task['anchor'][0], 'spot', 'orderbook'])
            if anchor_key not in self.market_data:
                logger.warning("Didn't receive orderbook of {}".format(self.task['anchor'][0]))
                return

            m_symbol = self.task['median'][0]
            m_price_precision = get_price_precision(self.task, self.task['exchange'], m_symbol)
            m_amount_precision = get_amount_precision(self.task, self.task['exchange'], m_symbol)
            m_contract_type = self.task['contract_type'] if 'contract_type' in self.task else 'spot'
            m_base, m_quote = self.task['median'][1:3]
            m_direction = compute_direction(self.task, 'median')

            m_base_min_order_size, m_quote_min_order_size = get_min_order_size(self.task, self.task['exchange'],
                                                                               m_symbol)
            s_base, s_quote = self.task['symbol'][1:3]
            mid_coin = compute_mid_coin(self.task)
            post_only = True if self.task['trade_role'] == 'Maker' else False
            m_asks = orderbook['metadata']['asks']
            m_bids = orderbook['metadata']['bids']
            m_spread = m_asks[0][0] - m_bids[0][0]

            m_volume_filter = get_volume_filter(self.task)
            ob_s = 0
            if m_direction == Direction.SELL.value:
                m_price, m_size = price_filter_by_volume(m_asks, m_volume_filter)
                ob_s = cal_ob_avg_size(m_asks, 5)
            else:
                m_price, m_size = price_filter_by_volume(m_bids, m_volume_filter)
                ob_s = cal_ob_avg_size(m_bids, 5)


            if self.task['trade_role'] == 'Maker' and m_spread == m_price_precision:
                # only maker and no spread, set price to bid0 or ask0
                m_price = m_price
            else:
                m_price = m_price + m_price_precision if m_direction == Direction.BUY.value else m_price - m_price_precision
            m_price = format_price(m_price, m_price_precision)

            if not m_price or m_price <= 0:
                # price exception detect
                logger.warning('{} price is invalid: {}'.format(m_symbol, m_price))
                return
            if self.last_a_price <= 0:
                return
            if 'anchor_price' in self.task and self.task['anchor_price'] is not None:
                anchor_price = get_anchor_price(m_price, self.last_a_price, s_base, m_base, s_quote,
                                                self.task['anchor'][2])
                price_threshold = self.task['anchor_price']
                if not ((self.task['direction'] == Direction.BUY.value and anchor_price < price_threshold)
                        or (self.task['direction'] == Direction.SELL.value and anchor_price > price_threshold)):
                    # logger.warning("anchor price now is {}, didn't meat {}".format(anchor_price, price_threshold))
                    self.anchor_trading = False
                    return
            elif 'price_threshold' in self.task and self.task['price_threshold'] is not None:
                if not ((m_direction == Direction.BUY.value and m_price < self.task['price_threshold']) or (
                        m_direction == Direction.SELL.value and m_price > self.task['price_threshold'])):
                    # logger.warning("price now is {}, didn't meet {}".format(m_price, self.task['price_threshold']))
                    self.anchor_trading = False
                    return
            else:
                # not set price threshold
                anchor_price = None
                price_threshold = None

            self.anchor_trading = True
            self.last_m_price = m_price
            m_min_order_size = max(m_base_min_order_size, m_quote_min_order_size / m_price)
            # normally iceberg only have one active order, detect whether our order is at best place
            if len(self.pending_orders) > 0:
                for ref_id in self.pending_orders:
                    # still has order not in response
                    if self.pending_orders[ref_id]['symbol'] == self.task['median'][0]:
                        self.clear_timeout_pending_orders()
                        return

            if self.active_orders:
                for ref_id, order_info in self.active_orders.items():
                    if order_info['symbol'] == self.task['median'][0]:
                        # to do detect market abnormal behaviour
                        if order_info['price'] == m_asks[0][0] or order_info['price'] == m_bids[0][0]:
                            # order price is at best
                            return
                        else:
                            self.cancel_order(ref_id)
                            return

            tr_s = sum([_[4] for _ in self.trade_list])
            m_amount = cal_order_size_by_ob_tr(ob_s, tr_s, m_min_order_size, MAX_SIZE_BY_QUOTE[m_quote] / m_price)
            logger.info(f"ob_s{ob_s}; tr_s: {tr_s}; min_order_size: {m_min_order_size}; max_order_size: {MAX_SIZE_BY_QUOTE[m_quote]/m_price}; amount: {m_amount}")
            self.last_m_amount = m_amount
            # finish condition judge, 4 conditions

            if self.task['direction'] == Direction.SELL.value and self.task['currency_type'] == CurrencyType.BASE.value:
                # sell based base currency, normal condition
                diff = self.task['initial_balance'][s_base] - get_total_balance(t_iceberg_balance, s_base)
                residual_amount = self.task['total_size'] - diff
                logger.info("{} residual_amount:{} m_min_order_size: {}".format(m_base, residual_amount, m_min_order_size))
                if residual_amount < m_min_order_size:
                    self.median_stop_status = True
                    return

            if self.task['direction'] == Direction.SELL.value and self.task[
                'currency_type'] == CurrencyType.QUOTE.value:
                # quote currency change value
                mid_balance = get_total_balance(t_iceberg_balance, mid_coin) - self.task['initial_balance'][mid_coin]
                mid_balance_to_quote = mid_balance * self.last_a_price if mid_coin == self.task['anchor'][1] else mid_balance / self.last_a_price
                diff = get_total_balance(t_iceberg_balance, s_quote) + mid_balance_to_quote - \
                       self.task['initial_balance'][s_quote]
                residual_amount = self.task['total_size'] - diff

                s_price = get_anchor_price(m_price, self.last_a_price, s_base, m_base, s_quote, self.task['anchor'][2])

                residual_amount = residual_amount / s_price
                if residual_amount < m_min_order_size:
                    self.median_stop_status = True
                    return

            if self.task['direction'] == Direction.BUY.value and self.task['currency_type'] == CurrencyType.QUOTE.value:
                # sell based base currency, normal condition
                mid_balance = get_available_balance(t_iceberg_balance, mid_coin) - self.task['initial_balance'][mid_coin]
                residual_amount = mid_balance if mid_coin == m_base else mid_balance / m_price
                if residual_amount < m_min_order_size:
                    if self.anchor_stop_status:
                        self.on_iceberg_finish()
                        return
                    else:
                        logger.warning('Not enough for trading, minimal is {}'.format(m_min_order_size))
                        return

            if self.task['direction'] == Direction.BUY.value and self.task['currency_type'] == CurrencyType.BASE.value:
                mid_balance = get_available_balance(t_iceberg_balance, mid_coin) - self.task['initial_balance'][mid_coin]
                residual_amount = mid_balance if mid_coin == m_base else mid_balance / m_price
                if residual_amount < m_min_order_size:
                    if self.anchor_stop_status:
                        self.on_iceberg_finish()
                        return
                    else:
                        logger.warning('Not enough for trading, minimal is {}'.format(m_min_order_size))
                        return

            m_amount = min(residual_amount, m_amount)
            m_amount = format_amount(m_amount, m_amount_precision)

            self.send_order(self.task['exchange'], self.task['median'][0], m_contract_type, m_price, m_amount,
                            m_direction, OrderType.LIMIT.value,
                            self.task['account'], 'triangle_iceberg', None, post_only)
            self.last_order_time = datetime.now()

        if orderbook['exchange'] == self.task['exchange'] and orderbook['symbol'] == self.task['anchor'][0]:
            a_direction = compute_direction(self.task, 'anchor')
            a_symbol = self.task['anchor'][0]
            a_contract_type = self.task['contract_type'] if 'contract_type' in self.task else 'spot'
            a_price_precision = get_price_precision(self.task, self.task['exchange'], a_symbol)
            a_amount_precision = get_amount_precision(self.task, self.task['exchange'], a_symbol)
            a_base_min_order_size, a_quote_min_order_size = get_min_order_size(self.task, self.task['exchange'],
                                                                               a_symbol)
            a_base_currency, a_quote_currency = self.task['anchor'][1:3]
            s_base, s_quote = self.task['symbol'][1:3]

            mid_coin = compute_mid_coin(self.task)
            a_asks = orderbook['metadata']['asks']
            a_bids = orderbook['metadata']['bids']

            if a_direction == Direction.SELL.value:
                a_price, a_size = price_filter_by_volume(a_asks, None)
            else:
                a_price, a_size = price_filter_by_volume(a_bids, None)
            a_price = a_price - a_price_precision if a_direction == Direction.BUY.value else a_price + a_price_precision
            a_price = format_price(a_price, a_price_precision)
            if not a_price or a_price <= 0:
                # price abnormal
                logger.warning('price is invalid:  {}'.format(a_price))
                return
            self.last_a_price = a_price
            if self.last_m_price <= 0:
                return
            if 'transfer_coin' in self.task and self.task['transfer_coin'] is True:

                a_min_order_size = max(a_base_min_order_size, a_quote_min_order_size / a_price)
                a_amount = 0
                if self.task['direction'] == Direction.SELL.value and self.task[
                    'currency_type'] == CurrencyType.BASE.value:
                    mid_balance = get_available_balance(t_iceberg_balance, mid_coin) - \
                                  self.task['initial_balance'][mid_coin]
                    logger.info('mid_balance: ', mid_balance, get_available_balance(t_iceberg_balance, mid_coin),
                                a_price)
                    a_amount = mid_balance if mid_coin == a_base_currency else mid_balance / a_price
                    if a_amount < a_min_order_size:
                        if self.median_stop_status:
                            self.on_iceberg_finish()
                            return
                        else:
                            logger.warning(
                                'not enough {} for trade, now is {}, min order is {}'.format(mid_coin, mid_balance,
                                                                                             a_min_order_size))
                            return

                if self.task['direction'] == Direction.SELL.value and self.task[
                    'currency_type'] == CurrencyType.QUOTE.value:
                    mid_balance = get_available_balance(t_iceberg_balance, mid_coin) - \
                                  self.task['initial_balance'][mid_coin]
                    a_amount = mid_balance if mid_coin == a_base_currency else mid_balance / a_price
                    if a_amount < a_min_order_size:
                        if self.median_stop_status:
                            self.on_iceberg_finish()
                            return

                if self.task['direction'] == Direction.BUY.value and self.task[
                    'currency_type'] == CurrencyType.QUOTE.value:
                    if self.anchor_trading:
                        diff = self.task['initial_balance'][self.task['symbol'][2]] - \
                               t_iceberg_balance[self.task['symbol'][2]]['total']
                        residual_amount = self.task['total_size'] - diff
                        residual_amount = residual_amount if s_quote == a_base_currency else residual_amount / a_price
                        if residual_amount < a_min_order_size:
                            # execution done
                            self.anchor_stop_status = True
                            return

                        a_maintain = get_maintain_amount(self.last_m_amount, self.last_m_price, self.task['median'][1],
                                                         mid_coin)
                        mid_balance = get_total_balance(t_iceberg_balance, mid_coin) - \
                                      self.task['initial_balance'][mid_coin]
                        a_amount = a_maintain - mid_balance

                        a_amount = a_amount if mid_coin == a_base_currency else a_amount / a_price

                        a_amount = min(residual_amount, a_amount)

                        a_amount = format_amount(a_amount, a_amount_precision)
                        if a_amount < a_min_order_size:
                            logger.info('There has enough {} {}'.format(mid_balance, mid_coin))
                            return

                if self.task['direction'] == Direction.BUY.value and self.task[
                    'currency_type'] == CurrencyType.BASE.value:
                    if self.anchor_trading:
                        mid_balance = get_total_balance(t_iceberg_balance, mid_coin) - \
                                      self.task['initial_balance'][mid_coin]
                        mid_balance_to_base = mid_balance * self.last_m_price if mid_coin == self.task['median'][1] else mid_balance / self.last_m_price
                        diff = get_total_balance(t_iceberg_balance, s_base) + mid_balance_to_base - \
                               self.task['initial_balance'][s_base]
                        residual_amount = self.task['total_size'] - diff
                        residual_amount_to_mid = residual_amount * self.last_m_price if s_base == self.task['median'][1] else residual_amount / self.last_m_price
                        residual_amount_to_anchor = residual_amount_to_mid if mid_coin == a_base_currency else residual_amount_to_mid / a_price

                        if residual_amount_to_anchor < a_min_order_size:
                            # execution done
                            self.anchor_stop_status = True
                            return

                        a_maintain = get_maintain_amount(self.last_m_amount, self.last_m_price, self.task['median'][1],
                                                         mid_coin)
                        a_amount = a_maintain - mid_balance
                        a_amount = a_amount if mid_coin == a_base_currency else a_amount / a_price
                        a_amount = min(residual_amount_to_anchor, a_amount)
                        if a_amount < a_min_order_size:
                            logger.info('There has enough {} {}'.format(mid_balance, mid_coin))
                            return

                a_amount = format_amount(a_amount, a_amount_precision)
                if self.pending_orders and len(self.pending_orders) > 0:
                    for ref_id in self.pending_orders:
                        # still has order not in response
                        if self.pending_orders[ref_id]['symbol'] == self.task['anchor'][0]:
                            return

                if len(self.active_orders) > 0:
                    for ref_id, order_info in self.active_orders.items():
                        if order_info['symbol'] == self.task['anchor'][0]:
                            # to do detect market abnormal behaviour
                            if order_info['price'] == a_asks[0][0] or order_info['price'] == a_bids[0][0]:
                                # order price is at best
                                return
                            else:
                                self.cancel_order(ref_id)
                                return

                self.send_order(self.task['exchange'], self.task['anchor'][0], a_contract_type, a_price, a_amount,
                                a_direction, OrderType.LIMIT.value,
                                self.task['account'], 'triangle_iceberg')

    def on_response(self, response):
        super().on_response(response)

    def on_timer(self):
        super().on_timer()

    def on_iceberg_finish(self):
        """
       t-iceberg finish func; wait orders in pending_orders and active_orders executed
        """
        if len(self.pending_orders) > 0 or len(self.active_orders) > 0:
            time.sleep(3)
            logger.info('There still have orders not handled, sleep 3 seconds!')
            return
        logger.info('Triangle Iceberg has finished!')
        self.status = TaskStatus.FINISHED.value
        self.status_msg = 'Triangle Iceberg has finished'
        self.on_finish()

    def on_finish(self):
        super().on_finish()
