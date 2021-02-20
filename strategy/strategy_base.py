# encoding: utf-8

import json
from datetime import datetime

from config.config import sentry
from config.enums import *
from util.logger import logger
from util.util import *


class StrategyBase:

    def __init__(self):
        self.task_id = ''
        self.task = {}  # task, dict; get from ui
        self.strategy_id = ''
        self.config = {}  # config info
        self.valid_symbols = {}
        self.handler = None

        self.orders = None  # 所有的发单记录
        self.pending_orders = None  # 已经发单到PDT, 但是暂时还没有收到PDT回复, 没有order_id, 以ref_id为key
        self.active_orders = None  # 发单收到PDT回复, 有order_id, 需要定时inspect查询状态, 以ref_id为key
        self.finished_orders = None  # 所有已经完结的订单, filled/cancelled/rejected

        self.balance = {}  # 策略维护自己独立的balance, 根据回报计算
        self.global_balance = {}  # 绑定strategy_master中的全局balance, 已经合并balance推送和根据回报计算的balance, 根据exchange自动调整
        self.status = TaskStatus.RUNNING.value  # algo status
        self.status_msg = '任务正在运行'  # algo status msg
        self.time_control = {}  # now only support inspect time frequency control

        self.deal_size = 0  # deal size of algo
        self.deal_size_not_updated_time = 0  # deal size not updated time
        self.deal_size_snapshot = 0  # deal size last value
        self.attention = False  # default is False, for no diff of deal_size
        self.deal_size_alarm_interval = 600  # deal size alarm interval
        self.current_price = None
        self.anchor_price = None
        self.finish_flag = False  # 在策略的on_finished函数中可以通过判断这个标志防止多次执行on_finished

    def on_init(self, config, task, master_ptr):
        """
        algo init: algo parameter validate; channel register and listen;
        balance init(compute by order response); log set; sentry set
        :param config: reference CONFIG_GLOBAL in config.py
        :param task: reference task_mock in driver
        :param master_ptr: reference strategy master
        :return:
        """
        self.config = config
        self.task = task
        self.handler = master_ptr
        self.task_id = task['task_id']
        self.strategy_id = task['strategy_id']

        logger.debug("on_init =>", self.strategy_id)

        self.valid_symbols[task['symbol'][0]] = task['symbol']
        if 'median' in task:
            self.valid_symbols[task['median'][0]] = task['median']
        if 'anchor' in task:
            self.valid_symbols[task['anchor'][0]] = task['anchor']

        for currency in self.task['initial_balance']:
            self.balance[currency] = {
                "total": self.task['initial_balance'][currency],
                "available": self.task['initial_balance'][currency],
                "reserved": 0, "shortable": 0
            }

        exch_acc = f"{task['exchange']}|{task['account']}"
        self.global_balance = self.handler.balance[exch_acc]
        if task['exchange'] in BALANCE_BY_ORDER_RES_EX and BALANCE_BY_ORDER_RES_EX[task['exchange']]:
            self.global_balance = self.handler.balance_by_order_res[exch_acc]

        self.time_control = {
            'on_time_count': 0,
            'inspect_time': config['TIME_INTERVAL'],
        }
        # 当交易所使用on_order_update的时候, 大幅度降低主动inspect的频率
        if task['exchange'] in ORDER_UPDATE_EX and ORDER_UPDATE_EX[task['exchange']]:
            self.time_control['inspect_time'] = 20 * config['TIME_INTERVAL']

        # Binding master orders
        self.orders = self.handler.orders[self.strategy_id]
        self.pending_orders = self.handler.pending_orders[self.strategy_id] if self.strategy_id in self.handler.pending_orders else {}
        self.active_orders = self.handler.active_orders[self.strategy_id] if self.strategy_id in self.handler.active_orders else {}
        self.finished_orders = self.handler.finished_orders[self.strategy_id] if self.strategy_id in self.handler.finished_orders else {}

    def on_book(self, market_data):
        """
        listen market data from pdt; check data delay, if more than 3, send alarm to desk quant
        :param market_data: receive from pdt, data format, plesase reference subset func
        """
        pass
    
    def on_quote_ready(self, quote):
        """
        quote = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: CONTRACT_TYPE.SPOT,
            data_type: MARKET_DATA.QUOTE,
            metadata: metadata,
            timestamp: YYYYMMDDHHmmssSSS
        };
        quote.metadata format
        spot:
        metadata = {
            [[side, price, count, size]]
        }
        future:
        metadata = {
            [[side, price, count, size in contracts, size in coins]]
        }

        notes: count means how many seperate orders are at this price. if zero delete the price
         from the orderbook. size means the updated size on this price for the orderbook.
        """
        logger.warning("Strategy base has no implementation for on_quote_ready")

    def on_orderbook_ready(self, orderbook):
        """
        orderbook = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: CONTRACT_TYPE.SPOT,
            data_type: MARKET_DATA.ORDERBOOK,
            metadata: metadata,
            timestamp: YYYYMMDDHHmmssSSS
        };
        orderbook.metadata format
        spot:
        metadata = {
            "bids" : [[price , amount]],
            "asks" : [[price , amount]],
            "timestamp": YYYYMMDDHHmmssSSS
        }
        future:
        metadata = {
            "bids" : [[price , amount in contracts, amount in coins]],
            "asks" : [[price , amount in contracts, amount in coins]],
            "timestamp": YYYYMMDDHHmmssSSS
        }
        """
        logger.warning("Strategy base has no implementation for on_orderbook_ready")

    def on_trade_ready(self, trade):
        """
        trade = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: CONTRACT_TYPE.SPOT,
            data_type: MARKET_DATA.TRADE,
            metadata: metadata,
            timestamp: YYYYMMDDHHmmssSSS
        };

        trade.metadata format
        spot:
        metadata = {
            [[id, timestamp, price, side, size]]
        }
        future:
        metadata = {
            [[id, timestamp, price, side, size, size in coin]]
        }
        notes: side can be "buy" or "sell". "buy" means the trade lifts the ask orderbook,
        "sell" the trade hits the bid orderbook.
        """
        logger.warning("Strategy base has no implementation for on_trade_ready")

    def on_funding_ready(self, funding):
        """
        funding = {
            exchange: "Bitfinex",
            symbol: "BTC",
            contract_type: CONTRACT_TYPE.SPOT,
            data_type: MARKET_DATA.FUNDING,
            metadata: metadata,
            timestamp: YYYYMMDDHHmmssSSS
        };
        funding.metadata format
        metadata = {
            "bids" : [[price , amount]],
            "asks" : [[price , amount]],
            "timestamp": YYYYMMDDHHmmssSSS
        }
        """
        logger.warning("Strategy base has no implementation for on_funding_ready")

    def on_ticker_ready(self, ticker):
        logger.warning("Strategy base has no implementation for on_ticker_ready")

    def on_index_ready(self, index):
        """
        index = {
            exchange: this.name,
            contract_type: CONTRACT_TYPE.SPOT,
            symbol: symbol,
            data_type: MARKET_DATA.INDEX,
            metadata: {
                index:price,
                timestamp: moment(timestamp).format("YYYYMMDDHHmmssSSS")
            },
            timestamp: utils._util_get_human_readable_timestamp()
            };
        """
        logger.warning("Strategy base has no implementation for on_index_ready")

    def on_kline_ready(self, kline):
        """
        kline = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: CONTRACT_TYPE.SPOT,
            data_type: MARKET_DATA.KLINE,
            metadata: metadata,
            range: "5min",
            subscribed_kline_size: 200
            timestamp: YYYYMMDDHHmmssSSS
        };
        kline.metadata format
        metadata = {
            [[timestamp, open, close, high, low, volume]]
        }
        :param kline:
        :return:
        """
        logger.warning("Strategy base has no implementation for on_kline_ready")

    def on_response(self, response):
        """
        这里已经整合了目前的回报信息, 提供统一的格式
        response = {
            'strategy_id': 'ICEBERG_Binance_BTCUSDT_20190725152929',
            'ref_id': 'uuid',
            'action': OrderActions.SEND.value,
            'task_id': 'XXXXXXXXXXX',
            'exchange': 'Binance',
            'symbol': 'BTCUSDT',
            'contract_type': 'spot',
            'timestamp': 'YYYYMMDDHHmmssSSS',
            'status': OrderStatus.FILLED.value,
            'direction': 'Buy',
            'original_amount': 1.0,
            'original_price': 100.0,
            'filled': 0,
            'avg_executed_price': 0.0
            若发单失败, 则无下列信息--------------
            'order_id': 'uuid',
            'account_id': 'trader1'
        }
        """
        filled_info = f'{response["filled"]}@{response["avg_executed_price"]}'
        logger.info(f'OrderResponse => {response["strategy_id"]} {response["ref_id"]} {response["action"]} '
                    f'{response["exchange"]} {response["symbol"]} {response["contract_type"]} {response["direction"]} '
                    f'{response["original_amount"]}@{response["original_price"]} {filled_info} '
                    f'{response["status"]} {response["timestamp"]}')

        self.balance_management(response)

    def on_timer(self):
        """
        regular execute
        """
        if self.task['algorithm'] == Algorithms.TRIANGLE_TWAP.value or self.task['algorithm'] == Algorithms.TRIANGLE_ICEBERG.value:
            self.check_middle_size()
        self.check_end_time()

    def check_end_time(self):
        if self.task['test_mode']:
            return
        if self.status == TaskStatus.PAUSED.value:
            return
        if 'end_time' in self.task and self.task['end_time'] is not None:
            if (datetime.now() - str_to_datetime(self.task['end_time'])).total_seconds() > 300:
                self.alarm('Execution has not ended after end_time', AlarmCode.EXECUTE_ABNORMAL.value)

    def check_middle_size(self):
        if self.task['test_mode']:
            return
        mid_coin = get_mid_coin_from_triangle_pair(self.task["symbol"], self.task["median"], self.task["anchor"])
        mid_initial = self.task['initial_balance'][mid_coin]
        mid_balance_now = get_total_balance(self.balance, mid_coin)
        if mid_coin in MAX_SIZE_BY_QUOTE and mid_balance_now - mid_initial > MAX_SIZE_BY_QUOTE[mid_coin]:
            self.alarm(f'mid_coin balance abnormal: {mid_balance_now - mid_initial} {mid_coin}', AlarmCode.EXECUTE_ABNORMAL.value)

    def on_finish(self):
        """
        invoked when algo when finished、run exception, ui deleted;
        when in finished condition, send report_email to trades
        """
        if not self.finish_flag:
            self.finish_flag = True

    def send_order(self, exchange=None, symbol=None, contract_type=None, price=None, quantity=None, direction=None,
                   order_type=None, account_id=None, strategy_key=None, delay=None, post_only=False):
        """
        send order
        :param exchange: exchange|'Binance'
        :param symbol: symbol|'BTCUSDT'
        :param contract_type: string | 'spot'
        :param price: number | 8822.45
        :param quantity: number | 5.24
        :param direction: string | Buy or Sell
        :param order_type: string | 'Fak' or 'Limit'
        :param account_id: string | 'trader01'
        :param strategy_key: string, | 'iceberg'
        :param delay: int, used in fak, unit is ms | 6000(6s)
        :param post_only: bool, true means only send 'maker' order
        """
        _, base, quote = self.valid_symbols[symbol]
        increase_reserved_amount(self.balance, base, quote, direction, quantity, price)
        self.handler.send_order(exchange, symbol, contract_type, price, quantity, direction,
                                order_type, account_id, strategy_key, delay, post_only, self.strategy_id)

    def inspect_order(self, ref_id):
        """
        inspect order by ref_id, order info is stored in active_orders ,key is ref_id
        :param ref_id: type string
        """
        self.handler.inspect_order(self.strategy_id, ref_id)

    def clear_timeout_pending_orders(self):
        self.handler.clear_timeout_pending_orders(self.strategy_id)

    def cancel_order(self, ref_id):
        """
        cancel order by ref_id, order info is stored in active_orders ,key is ref_id
        :param ref_id: type string
        :return:
        """
        self.handler.cancel_order(self.strategy_id, ref_id)

    def alarm(self, alarm_msg, alarm_code='011111'):
        """
        code
        1	1	3	4	5	6	PIC	Comment
        0	1	*	*	except 1	*	Daniel	This is for algo internal error, now only daniel can receive and only send mail
           1	0	0	1	0	Luke、Michale、Tony、Wayne	This is for margin alarm(margin leverage more than threhold  last for more than 2h), mail and phone
           2	except 1	except 1	except 1	except 1	Traders and Daniel and Tongmin	This is for algo imb/pnl  alarm, phone for pic and mail for trading.alarms(all mm projects)
           2	1	1	1	1	Tongmin	This is for otc price data feed, phone for tongmin, and mail for daniel and tongmin
           3	[2,3]	3	1	0	Traders	This is for price alarm, phone to traders and send mail to trading.alarms
           3	*	*	*	*	Traders and Daniel and Tongmin	This is for algo volume alarm,  phone for pic and mail for trading.alarms(all mm projects)
           5	*	*	*	*	Tongmin and Daniel	This is for algo internal error, now only daniel and tongmin can receive and only send mail
           9	1	*	*	*	Traders and Daniel and Tongmin	This is for price alarm, only send mail to trading.alarms(all mm projects)
           9	2	*	*	*	Traders and Daniel and Tongmin	This is for spread alarm, only send mail to trading.alarms(all mm projects)
           9	3	*	*	*	Traders and Daniel and Tongmin	This is for depth alarm, only send mail to trading.alarms(all mm projects)
           9	9	*	*	*	Traders and Daniel and Tongmin	This is for feed alarm(data not received for 5 mins), only send mail to trading.alarms(all mm projects)
           6	1	1	1	1	Tongmin	This is for gaea MM project,  mail and phone


        traders includes Sean, Kelvin, Jay, Ivan, Sarah, Xiaopeng, Anthony
        first digit 0 means MM， 1 means basis strategy, and 8 means db
        price alarm sometimes solely setted for particular person
        trading.alarms includes Tongmin 、Daniel 、Traders 、Wayne、Thomas
        pdttradingalert includes Guoquan、Rongkai、Luke、Thomas、Wayne
        """
        self.handler.alarm(alarm_msg, alarm_code)

    def update_status(self, status, msg):
        """
        push algo status to UI
        :param status: type dict
        :param msg: type string
        """
        self.status_msg = msg
        if status == TaskStatus.WARNING.value:
            self.status_msg = 'warning|' + self.status_msg
            self.handler.status_msg = self.status_msg
            self.handler.last_warning_time = datetime.now()
        else:
            self.status = status

    def inspect_order_on_time(self):
        self.time_control['on_time_count'] += 1
        if self.time_control['on_time_count'] * self.config['TIME_INTERVAL'] < self.time_control['inspect_time']:
            return
        self.time_control['on_time_count'] = 0
        for ref_id in self.active_orders:
            self.inspect_order(ref_id)

    def process_frequency_error(self):
        self.time_control['inspect_time'] += self.config['TIME_INTERVAL']
        logger.warning(f'WARNING => 发现交易所限频, 降低{self.strategy_id}inspect频率至{self.time_control["inspect_time"]}秒一次')

    def check_deal_size(self):
        if self.status == TaskStatus.PAUSED.value:
            return
        if self.deal_size_snapshot != self.deal_size:
            # order has update
            self.deal_size_snapshot = self.deal_size
            self.attention = False
            self.deal_size_not_updated_time = 0
            return

        if self.deal_size_not_updated_time > self.deal_size_alarm_interval:
            self.attention = True
            self.deal_size_not_updated_time = 0

            if self.task['test_mode']:
                return

            msg = f"Deal size not updated for 10 minutes"
            if self.task['algorithm'] not in [Algorithms.ICEBERG.value, Algorithms.SAMPLE.value]:
                if self.task['algorithm'] == Algorithms.TWAP.value:
                    self.deal_size_alarm_interval += 300

                self.alarm(msg, AlarmCode.DEAL_SIZE_NOT_UPDATED.value)
            elif self.task['algorithm'] in [Algorithms.ICEBERG.value] and not self.active_orders:
                self.alarm(msg, AlarmCode.DEAL_SIZE_NOT_UPDATED.value)

        if not self.current_price:
            # market data didn't receive
            self.deal_size_not_updated_time += self.config['TIME_INTERVAL']
            return

        anchor_price_exist = 'anchor_price' in self.task and self.task['anchor_price'] is not None
        if self.task['direction'] == Direction.BUY.value:
            if anchor_price_exist and self.task['anchor_price'] > self.current_price:
                self.deal_size_not_updated_time += self.config['TIME_INTERVAL']
            if self.task['price_threshold'] is None or self.task['price_threshold'] > self.current_price:
                self.deal_size_not_updated_time += self.config['TIME_INTERVAL']
        else:
            if anchor_price_exist and self.task['anchor_price'] < self.current_price:
                self.deal_size_not_updated_time += self.config['TIME_INTERVAL']
            if self.task['price_threshold'] is None or self.task['price_threshold'] < self.current_price:
                self.deal_size_not_updated_time += self.config['TIME_INTERVAL']

    def balance_management(self, response):
        ref_id = response['ref_id']
        # 如果订单已经处于完成状态, 无需重复处理, 直接跳过
        if ref_id in self.finished_orders:
            return

        _, base, quote = self.valid_symbols[response["symbol"]]
        origin_order = self.orders[response["ref_id"]]

        ret = balance_management_common_process(self.balance, response, base, quote, origin_order)
        if ret:
            logger.debug("Strategy Balance => ", json.dumps(self.balance))
