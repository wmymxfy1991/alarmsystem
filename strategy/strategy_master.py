import copy
from datetime import datetime

from config.config import sentry
from config.enums import *
from util.logger import logger
from util.util import *
from strategy.iceberg import Iceberg
from strategy.sample import Sample
from strategy.triangle_iceberg import TriangleIceberg
from strategy.triangle_twap import TriangleTwap
from strategy.twap import Twap
from strategy.vwap import Vwap

STRATEGYS = {
    'SAMPLE': Sample,
    'VWAP': Vwap,
    'TWAP': Twap,
    'T-TWAP': TriangleTwap,
    'ICEBERG': Iceberg,
    'T-ICEBERG': TriangleIceberg
}


class StrategyMaster:
    def __init__(self):
        self.ip = get_ip()  # server ip where algo is running
        self.pid = get_pid()  # pid info of algo
        self.requests = []  # requests queue
        self.strategies = {}
        self.task = {}  # task, dict; get from ui
        self.task_id = ''  # task_id
        self.config = {}  # config info
        self.strategy_name = ''  # strategy_name, must be enabled in pdt, otherwise can't do the trade
        self.trade_request_key = ''  # trade key for push trade info (orders) to pdt
        self.subscribe_key = {'trade': {}, 'market_data': {}, 'balance': {}, 'order_update': {}}

        self.valid_exchanges = {}
        self.valid_account_id = {}
        self.valid_symbols = {}

        self.balance = {}  # balance snapshot, received from pdt
        self.balance_by_order_res = {}  # balance status, computed by order response, init value is 0
        self.balance_status = {}  # default is False, if true means having received balance data from pdt
        self.status = TaskStatus.RUNNING.value  # algo status
        self.status_msg = '任务正在运行'  # algo status msg

        self.order_count = 0  # order cum count, start from 0
        self.orders = {}  # 所有策略的发单记录
        self.pending_orders = {}  # 已经发单到PDT, 但是暂时还没有收到PDT回复, 没有order_id, 以ref_id为key
        self.active_orders = {}  # 发单收到PDT回复, 有order_id, 需要定时inspect查询状态, 以ref_id为key
        self.finished_orders = {}  # 所有已经完结的订单, filled/cancelled/rejected
        self.strategy_order_map = {}  # store order_id by send_order, used to map order_id from on_order_update

        self.error_count = 0
        self.last_warning_time = datetime.now()

    def on_init(self, config, task):
        logger.debug(json.dumps(task))
        for strategy_id in task['strategies']:
            st_task = task['strategies'][strategy_id]
            st_task['strategy_id'] = strategy_id
            self.init_master_orders(strategy_id)
            update_strategy_conf(st_task, task)
            result, info = task_validate(st_task)
            if not result:
                logger.error(info)
                self.error_handler(TaskStatus.ERROR.value, info)

        self.config = config
        self.task = task
        self.task_id = task['task_id']
        self.strategy_name = config['STRATEGY_NAME']
        self.trade_request_key = f'{IntercomScope.TRADE.value}:{self.strategy_name}_request'
        if task['test_mode']:
            self.trade_request_key = 'Test' + self.trade_request_key

        # 初始化计算balance
        for exch_acc in self.task['initial_balance']:
            self.balance[exch_acc] = {}
            self.balance_by_order_res[exch_acc] = {}
            self.balance_status[exch_acc] = False
            for currency in self.task['initial_balance'][exch_acc]:
                self.balance_by_order_res[exch_acc][currency] = {
                    "total": self.task['initial_balance'][exch_acc][currency],
                    "available": self.task['initial_balance'][exch_acc][currency],
                    "reserved": 0, "shortable": 0
                }
                self.balance[exch_acc][currency] = {
                    "total": self.task['initial_balance'][exch_acc][currency],
                    "available": self.task['initial_balance'][exch_acc][currency],
                    "reserved": 0, "shortable": 0
                }

        # 从文件缓存中回滚历史订单信息
        order_path = os.path.join(ROOT_PATH, 'orders', f'{self.task_id}.json')
        if os.path.isfile(order_path):  # 当存在订单缓存文件的时候, 重载进内存, 并删除文件
            logger.debug('Start loading order cache file...')
            with open(order_path, 'r') as f:
                order_his_data = json.load(f)
                self.pending_orders = order_his_data['pending_orders']
                self.rebuild_orders(self.pending_orders)
                self.active_orders = order_his_data['active_orders']
                self.rebuild_orders(self.active_orders)
                self.finished_orders = order_his_data['finished_orders']
                self.rebuild_orders(self.finished_orders)
            os.remove(order_path)
            logger.debug('Delete order cache file success')

        for strategy_id in task['strategies']:
            st_task = task['strategies'][strategy_id]
            update_strategy_conf(st_task, task)
            self.set_valid_infos(st_task)
            self.prepare_with_subscription(st_task)
            strategy = STRATEGYS[st_task['algorithm']]()
            strategy.on_init(config, st_task, self)
            self.strategies[strategy_id] = strategy
 
        subscribe_obj = {}
        # 执行订阅行情
        for mkey in self.subscribe_key['market_data']:
            subscribe_obj[mkey] = self.on_book
        # 执行订阅交易回报
        for tkey in self.subscribe_key['trade']:
            subscribe_obj[tkey] = self.on_response_process
        # 执行订阅balance回报
        for bkey in self.subscribe_key['balance']:
            subscribe_obj[bkey] = self.on_balance_response
        # 执行订阅order_update回报
        for okey in self.subscribe_key['order_update']:
            subscribe_obj[okey] = self.on_order_update

        self.send_request(['', subscribe_obj], rtype='Subscribe')

        # 发送订阅币对行情的请求
        market_data_sub = [k.split(':')[1] for k in self.subscribe_key['market_data'].keys()]
        self.send_request([f'{IntercomScope.MARKET.value}:{IntercomChannel.SUBSCRIBE_REQUEST.value}', market_data_sub])

        # 发送存在阿里云oss的日志信息
        self.send_command_response({'type': Command.START.value, 'client_id': 0}, logger.file_link)

        # 可以自定义需要展示的数据, 除了异常捕捉, 还可以主动向server发消息: sentry.captureMessage('simple test')
        sentry.user_context({
            'ip': self.ip,
            'pid': self.pid,
            'git': get_git_msg(),
            'task_id': self.task_id,
            'task': self.task
        })

    def rebuild_orders(self, his_orders):
        for strategy_id in his_orders:
            for ref_id in his_orders[strategy_id]:
                order_count = int(str.split(ref_id, '_')[-1])
                if order_count > self.order_count:
                    self.order_count = order_count
                self.orders[strategy_id][ref_id] = his_orders[strategy_id][ref_id]

    def on_order_update(self, updated_order):
        """
        on_order_update, here set ref_id maps to ex_sy_orderid
        updated_order = {
            exchange: 'Binance',
            symbol: 'BTCUSDT',
            contract_type: "spot",
            metadata: {
                account_id: account_id,
                order_id: 'order_id',
                result: true,
            },
            timestamp: '20191111134330877',
            order_info:  {
                original_amount: 50,
                filled: 30,
                avg_executed_price: 2.8,
                status: 'cancelled'
            }
        }
        """
        updated_order = json.loads(updated_order['data'])

        ex_sy_order_id = f"{updated_order['exchange']}|{updated_order['symbol']}|{updated_order['metadata']['order_id']}"
        if ex_sy_order_id in self.strategy_order_map:
            order_update_key = f"{IntercomScope.TRADE.value}:{self.task['exchange']}|{self.task['account']}"
            self.subscribe_key['order_update'][order_update_key]['update_time'] = datetime.now()
            maped_info = self.strategy_order_map[ex_sy_order_id]
            origin_order = self.orders[maped_info['strategy_id']][maped_info['ref_id']]
            response = {
                'strategy_id': maped_info['strategy_id'],
                'ref_id': maped_info['ref_id'],
                'action': RequestActions.INSPECT_ORDER.value,
                'exchange': updated_order['exchange'],
                'symbol': updated_order['symbol'],
                'contract_type': updated_order['contract_type'],
                'direction': origin_order['direction'],
                'original_amount': updated_order['order_info']['original_amount'],
                'original_price': origin_order['price'],
                'status': updated_order['order_info']['status'],
                'timestamp': updated_order['timestamp'],
                'filled': updated_order['order_info']['filled'],
                'avg_executed_price': updated_order['order_info']['avg_executed_price']
            }
            origin_order['update_time'] = get_datetime(updated_order['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            self.on_response(response)

    def set_valid_infos(self, st_task):
        self.valid_exchanges[st_task['exchange']] = True
        self.valid_account_id[st_task['account']] = True
        self.valid_symbols[st_task['symbol'][0]] = st_task['symbol']
        if 'median' in st_task:
            self.valid_symbols[st_task['median'][0]] = st_task['median']
        if 'anchor' in st_task:
            self.valid_symbols[st_task['anchor'][0]] = st_task['anchor']

    def init_master_orders(self, strategy_id):
        self.orders.setdefault(strategy_id, {})
        self.pending_orders.setdefault(strategy_id, {})
        self.active_orders.setdefault(strategy_id, {})
        self.finished_orders.setdefault(strategy_id, {})

    def prepare_with_subscription(self, st_task):
        test_mode = st_task['test_mode']
        trade_response_key = f'{IntercomScope.TRADE.value}:{self.strategy_name}_response'
        balance_response_key = f'{IntercomScope.POSITION.value}:{st_task["exchange"]}|{st_task["account"]}'
        balance_request_key = f'{IntercomScope.POSITION.value}:Poll Position Request'

        # 预处理: 订阅账户的Balance信息
        if test_mode:
            trade_response_key = 'Test' + trade_response_key
            balance_response_key = 'Test' + balance_response_key
            balance_request_key = 'Test' + balance_request_key
            self.send_request([balance_request_key, st_task])
        else:
            self.send_request([balance_request_key, f"{st_task['exchange']}|{st_task['account']}"])
            # 订阅OrderUpdate信息
            if st_task['exchange'] in ORDER_UPDATE_EX.keys() and ORDER_UPDATE_EX[st_task['exchange']]:
                order_update_key = f"{st_task['exchange']}|{st_task['account']}"
                self.send_request([
                    f'{IntercomScope.TRADE.value}:{IntercomChannel.ORDER_UPDATE_SUBSCRIPTION_REQUEST.value}',
                    json.dumps(order_update_key)
                ])
                self.subscribe_key['order_update'][f'{IntercomScope.TRADE.value}:{order_update_key}'] = {
                    'update_time': datetime.now(),
                    'count': 0
                }

        # 预处理: balance回报订阅
        self.subscribe_key['balance'][balance_response_key] = True

        # 预处理: 交易数据订阅
        self.subscribe_key['trade'][trade_response_key] = True

        # 预处理: 行情订阅
        if st_task['algorithm'] not in [Algorithms.TRIANGLE_ICEBERG.value, Algorithms.TRIANGLE_TWAP.value]:
            market_key = f'{IntercomScope.MARKET.value}:{st_task["exchange"]}|{st_task["symbol"][0]}|spot|orderbook|20'
            self.subscribe_key['market_data'][market_key] = {
                'update_time': datetime.now(),
                'count': 0
            }

        if 'median' in st_task:
            median_key = f'{IntercomScope.MARKET.value}:{st_task["exchange"]}|{st_task["median"][0]}|spot|orderbook|20'
            self.subscribe_key['market_data'][median_key] = {
                'update_time': datetime.now(),
                'count': 0
            }

        if 'anchor' in st_task:
            anchor_key = f'{IntercomScope.MARKET.value}:{st_task["exchange"]}|{st_task["anchor"][0]}|spot|orderbook|20'
            self.subscribe_key['market_data'][anchor_key] = {
                'update_time': datetime.now(),
                'count': 0
            }

        if st_task['algorithm'] in [Algorithms.ICEBERG.value]:  # now only iceberg uses trade data
            trade_key = f'{IntercomScope.MARKET.value}:{st_task["exchange"]}|{st_task["symbol"][0]}|spot|trade'
            self.subscribe_key['market_data'][trade_key] = {
                'update_time': datetime.now(),
                'count': 0
            }

        if st_task['algorithm'] in [Algorithms.VWAP.value]:
            kline_key = f'{IntercomScope.MARKET.value}:{st_task["exchange"]}|{st_task["symbol"][0]}|spot|kline||1m'
            self.subscribe_key['market_data'][kline_key] = {
                'update_time': datetime.now(),
                'count': 0
            }

    def on_book(self, market_data):
        market_data = json.loads(market_data['data'])
        try:
            if market_data['symbol'] not in self.valid_symbols or market_data['exchange'] not in self.valid_exchanges:
                return  # 不是我们订阅的行情, 忽略之
            if not str(market_data['timestamp']).isdigit() or len(str(market_data['timestamp'])) != 17:
                logger.error(f"timestamp value error: {market_data['timestamp']}")
                return
            if market_data['data_type'] == MarketDataType.ORDERBOOK.value:
                self.cal_current_price(market_data)
                if not self.check_trade_precision(market_data):
                    return
            if self.status == TaskStatus.PAUSED.value:
                return

            # 当前行情的时效性不够
            if not market_data_validate(market_data, 3):
                msg = f"{market_data['exchange']} {market_data['symbol']} market info didn't update from " \
                      f"{get_datetime(market_data['timestamp'])} to {datetime.now()}"
                logger.file(msg)
                self.alarm(msg, AlarmCode.DATA_OUTDATED.value)

            for strategy_id in self.strategies:
                st = self.strategies[strategy_id]
                if market_data['symbol'] not in st.valid_symbols:
                    continue

                if market_data['data_type'] == MarketDataType.QUOTE.value:
                    st.on_quote_ready(market_data)
                elif market_data['data_type'] == MarketDataType.ORDERBOOK.value:
                    key = '|'.join([market_data['exchange'], market_data['symbol'], market_data['contract_type'],
                                    market_data['data_type'], '20'])
                    key = f'{IntercomScope.MARKET.value}:{key}'
                    self.subscribe_key['market_data'][key]['update_time'] = datetime.now()
                    st.on_orderbook_ready(market_data)
                elif market_data['data_type'] == MarketDataType.TRADE.value:
                    key = '|'.join([market_data['exchange'], market_data['symbol'], market_data['contract_type'],
                                    market_data['data_type']])
                    key = f'{IntercomScope.MARKET.value}:{key}'
                    self.subscribe_key['market_data'][key]['update_time'] = datetime.now()
                    st.on_trade_ready(market_data)
                elif market_data['data_type'] == MarketDataType.FUNDING.value:
                    st.on_funding_ready(market_data)
                elif market_data['data_type'] == MarketDataType.INDEX.value:
                    st.on_index_ready(market_data)
                elif market_data['data_type'] == MarketDataType.KLINE.value:
                    key = '|'.join([market_data['exchange'], market_data['symbol'], market_data['contract_type'],
                                    market_data['data_type'], '|1m'])
                    key = f'{IntercomScope.MARKET.value}:{key}'
                    self.subscribe_key['market_data'][key]['update_time'] = datetime.now()
                    st.on_kline_ready(market_data)
                elif market_data['data_type'] == MarketDataType.QUOTETICKER.value:
                    st.on_quote_ticker_ready(market_data)
                else:
                    logger.error(market_data)
                    logger.error(f'wrong type of market data: {market_data["data_type"]}')
        except Exception as e:
            logger.error(e)
            sentry.captureException()
    
    def check_task_status(self):
        # 如果所有子策略status都是finished，则认为task结束
        finish_status = [self.strategies[algo].status == TaskStatus.FINISHED.value for algo in self.strategies]
        if all(finish_status):
            self.status = TaskStatus.FINISHED.value
            self.status_msg = "Task has finished"
            self.on_finish()
            
    def on_timer(self):
        # 定时检查task是否结束
        self.check_task_status()
        # 定时对active_order查询状态
        self.inspect_order_on_time()
        # 定时检查市场数据是否更新
        self.check_market_data()
        # 定时检查是否成交
        self.check_deal_size()
        # 定时向PDT UI后端返回目前的订单完成信息
        self.send_status()

        for strategy_id in self.strategies:
            self.strategies[strategy_id].on_timer()

    def check_deal_size(self):
        """
        check whether deal size is updated(value changed) in 10 minutes, if not, set self.attention to True
        """
        if self.status == TaskStatus.PAUSED.value:
            return

        for strategy_id in self.strategies:
            self.strategies[strategy_id].check_deal_size()

    def check_market_data(self):
        """
        check whether receive orderbook data in 5 minutes, when algo is in running status; if not, send alarm
        to desk quant
        :return:
        """
        if self.status == TaskStatus.PAUSED.value:
            # paused status need not to check market data
            return

        channels = []
        for channel in self.subscribe_key['market_data']:
            interval = 60 * 5 if 'trade' not in channel else 60 * 60
            channel_info = self.subscribe_key['market_data'][channel]
            if (datetime.now() - channel_info['update_time']).total_seconds() > interval:
                # market data status check
                channel_info['update_time'] = datetime.now()
                channel_info['count'] += 1

                msg = f"{channel} didn't receive market data from pdt for 5 mins"
                if channel_info['count'] == 1:
                    self.alarm(msg, AlarmCode.DATA_UNRECEIVED.value)

        if len(channels) > 0:
            md_channels = [sub.split(':')[1] for sub in channels]
            self.send_request([f'{IntercomScope.MARKET.value}:{IntercomChannel.SUBSCRIBE_REQUEST.value}', md_channels])

        for channel in self.subscribe_key['order_update']:
            # on order update status check
            channel_info = self.subscribe_key['order_update'][channel]
            if (datetime.now() - channel_info['update_time']).total_seconds() > 60 * 5:
                channel_info['update_time'] = datetime.now()
                channel_info['count'] += 1
                self.send_request([
                    f'{IntercomScope.TRADE.value}:{IntercomChannel.ORDER_UPDATE_SUBSCRIPTION_REQUEST.value}',
                    json.dumps(channel.split(':')[1])
                ])
                msg = f"{channel} didn't receive order_update data from pdt for 5 mins"
                if channel_info['count'] == 1:
                    self.alarm(msg, AlarmCode.DATA_UNRECEIVED.value)

    def on_finish(self):
        self.send_status()
        for strategy_id in self.strategies:
            self.strategies[strategy_id].on_finish()

        if self.status == TaskStatus.FINISHED.value and not self.task["test_mode"]:
            try:
                trade_orders = self.get_trade_orders()
                send_execution_report_email(self.task, trade_orders)
            except Exception as e:
                logger.error("send execution report error:", e)
                sentry.captureException()
        self.save_all_order_info()
        self.send_request(['', ''], rtype='Exit')

    def on_command(self, command):
        """
        execute command from UI, then send command response to UI, now support 4 types
        1、pause: pause algo, set algo status to pause;
        2、resume: resume algo, set algo status to running;
        3、delete: delete algo, stop algo process, invoke on_finish() of algo;
        4、cancel: cancel active order of algo; in this case, algo must be in pause status, control in UI;
        5、send_order: receive command to send order;
        6、statistics: orders statistics info
        :param send_order command: {
            "type": "send_order",
            "client_id": 1570863741413,
            "task_id": "ICEBERG_Binance_BTCUSDT_20190725152929",
            "exchange": "Binance",  # 可以不填, 默认 task 中的交易所
            "symbol": "BTCUSDT",  # 可以不填, 默认 task 中的交易币对
            "contract_type": "spot",  # 可以不填, 默认 spot
            "price": 1000,
            "quantity": 1,
            "direction": "Buy",
            "order_type": "limit",  # 可以不填, 默认 limit
            "account": "trader1",  # 可以不填, 默认 task 中的交易账户
            "delay": 59000,  # 可以不填, 默认 59000
            "post_only": False  # 可以不填, 默认 False
        }
        :param statistics command: {
            'type':'statistics',
            'task_id':  'ICEBERG_Huobi_HTUSDT_20191203144448',
            'start_time': "2019-12-01 13:01:02",
            'end_time': "2019-12-04 13:01:25",
            'exchange_fee': 0.002,
            'service_fee': 0.01,
            'currency_type': 'Base',
            'client_id': 'test'
        }
        """
        command = command['data']
        print('command: ', command)
        try:
            data = json.loads(command)
            if data['task_id'] != self.task_id:
                return
            logger.debug(f"GetCommand => {json.dumps(data)}")
            if data['type'] == Command.PAUSE.value:
                self.status = TaskStatus.PAUSED.value
                self.update_status(TaskStatus.PAUSED.value, '任务暂停')
                logger.warning('Algorithm is paused')
                self.send_command_response(data)
            elif data['type'] == Command.RESUME.value:
                # 恢复的时候更新task字段
                if 'task' in data:
                    for strategy_id in data['task']['strategies']:
                        st_task = data['task']['strategies'][strategy_id]
                        st_task['strategy_id'] = strategy_id
                        update_strategy_conf(st_task, data['task'])
                        result, info = task_validate(st_task)
                        if not result:
                            self.send_command_response(data, info, False)
                            logger.error('参数更新失败: ' + info)
                            return
                        self.strategies[strategy_id].task = st_task
                    self.task = data['task']
                self.status = TaskStatus.RUNNING.value
                self.update_status(TaskStatus.RUNNING.value, '任务正在运行')
                logger.warning('Algorithm is resumed')
                self.send_command_response(data)
            elif data['type'] == Command.DELETE.value:
                # 处理程序的扫尾工作
                if not data['force_delete']:
                    if self.count_unfinished_order() > 0:
                        self.send_command_response(data, "当前还有订单在执行中, 强制删除订单可能会导致订单丢失, 请使用订单管理功能全部撤单之后再删除", False)
                        return

                self.status = TaskStatus.DELETED.value
                self.status_msg = "任务已被删除"
                self.cancel_all_order()
                self.send_command_response(data)
                self.on_finish()
            elif data['type'] == Command.OMS_SEND_ORDER.value:
                # 手工在交易所的发单
                if 'symbol' not in data:
                    self.send_command_response(data, "缺少symbol", False)
                    logger.error('Hand Order => Symbol not set')
                    return
                if 'strategy_id' not in data:
                    self.send_command_response(data, "没有设置strategy_id", False)
                    logger.error('Hand Order => strategy_id not set')
                    return
                if 'price' not in data or data['price'] <= 0:
                    self.send_command_response(data, "价格设置错误", False)
                    logger.error('Hand Order => Price Error')
                    return
                if 'quantity' not in data or data['quantity'] <= 0:
                    self.send_command_response(data, "交易量设置错误", False)
                    logger.error('Hand Order => Quantity Error')
                    return
                if 'direction' not in data or data['direction'] not in ('Buy', 'Sell'):
                    self.send_command_response(data, "买卖方向设置错误, 只能为 Buy/Sell", False)
                    logger.error('Hand Order => Direction Error, only Buy/Sell is valid')
                    return
                if 'trader' not in data:
                    self.send_command_response(data, "没有设置交易人员", False)
                    logger.error('Hand Order => Trader not set')
                    return
                strategy_id = data['strategy_id']
                st_task = self.task['strategies'][strategy_id]
                exch = data['exchange'] if 'exchange' in data else st_task['exchange']
                symbol = data['symbol']
                contract_type = data['contract_type'] if 'contract_type' in data else 'spot'
                order_type = data['order_type'] if 'order_type' in data else OrderType.LIMIT.value
                account_id = data['account'] if 'account' in data else st_task['account']
                delay = data['delay'] if 'delay' in data else None
                post_only = data['post_only'] if 'post_only' in data else False
                self.send_order(exch, symbol, contract_type, data['price'], data['quantity'], data['direction'],
                                order_type, account_id, f'hand_order|{data["trader"]}', delay, post_only, strategy_id)
                order_info = f"hand_order|{data['trader']} {exch} {account_id} {symbol} {data['direction']} {order_type} {data['quantity']}@{data['price']}"
                self.send_command_response(data, f"已经向交易所发送订单 => {order_info}")
            elif data['type'] == Command.OMS_CANCEL_ORDER.value:
                self.cancel_order(data['strategy_id'], data['ref_id'], True)
                self.send_command_response(data, '已经向交易所发送撤单请求')
            elif data['type'] == Command.OMS_INSPECT_ORDER.value:
                self.inspect_order(data['strategy_id'], data['ref_id'])
                self.send_command_response(data, '已经向交易所发送查单请求')
            elif data['type'] == Command.OMS_CANCEL_ALL_ORDER.value:
                # 撤销在交易所的所有挂单
                self.cancel_all_order()
                self.send_command_response(data, "已经向交易所发送撤销所有订单请求")
            elif data['type'] == Command.OMS_ORDER_STATUS.value:
                all_order_info = {
                    'pending_orders': self.pending_orders,
                    'active_orders': self.active_orders,
                    'finished_orders': self.finished_orders
                }
                self.send_command_response(data, all_order_info)
            elif data['type'] == Command.OMS_FINISHED_ORDERS.value:
                self.send_command_response(data, {
                    'link': '',
                    'finished_orders': self.finished_orders
                })
            elif data['type'] == Command.OMS_UNFINISHED_ORDERS.value:
                self.send_command_response(data, {
                    'pending_orders': self.pending_orders,
                    'active_orders': self.active_orders
                })
            elif data['type'] == Command.STATISTICS.value:
                strat_info = {}
                flag = False
                for strategy_id in data['strategies']:
                    if not self.finished_orders[strategy_id] and not self.active_orders[strategy_id]:
                        # finished_orders is None, there is no deal
                        strat_info[strategy_id] = {}
                        continue
                    flag = True
                    strat_info[strategy_id] = cal_orders(
                        dict(self.finished_orders[strategy_id], **self.active_orders[strategy_id]),
                        data['start_time'], data['end_time'],
                        data['strategies'][strategy_id]['exchange_fee'],
                        data['strategies'][strategy_id]['service_fee'],
                        data['strategies'][strategy_id]['currency_type']
                    )
                self.send_command_response(data, strat_info, flag)
            elif data['type'] == Command.EXPORT_STATISTICS.value:
                base, quote = self.get_base_quote_name(self.task['symbol'][0])
                ors_stat_info = cal_orders(dict(self.finished_orders, **self.active_orders), data['start_time'], data['end_time'],data['exchange_fee'],
                                            0 if 'service_fee' not in data or data['service_fee'] is None else data['service_fee'], data['currency_type'])
                link = create_export_statistics(data, ors_stat_info, self.current_price, self.task['price_threshold'], base, quote, self.task['test_mode'], self.task['total_size'])
                self.send_request([self.config['REDIS_TASK_COMMAND_RESP'], {
                    'task_id': data['task_id'],
                    'client_id': data['client_id'],
                    "type": Command.EXPORT_STATISTICS.value,
                    'msg': link
                }], rtype='', channel=PublishChannel.UI.value)

            elif data['type'] == Command.DOWNLOAD.value:
                trade_orders = self.get_trade_orders()
                all_links, _ = create_execution_report(data, trade_orders)
                print('all_links::：', all_links)
                self.send_request([self.config['REDIS_TASK_COMMAND_RESP'], {
                    'task_id': data['task_id'],
                    'client_id': data['client_id'],
                    "type": Command.DOWNLOAD.value,
                    'msg': all_links
                }], rtype='', channel=PublishChannel.UI.value)
        except Exception:
            sentry.captureException()

    def send_command_response(self, command, msg='', result=True):
        """
        send command response to UI
        """
        self.send_request([self.config['REDIS_TASK_COMMAND_RESP'], {
            'task_id': self.task_id,
            'type': command['type'],
            'client_id': command['client_id'],
            'status': self.status,
            "result": result,
            'msg': msg
        }], rtype='', channel=PublishChannel.UI.value)

    def update_status(self, status, msg):
        """
        push algo status to UI
        :param status: type dict
        :param msg: type string
        """
        self.status_msg = msg
        if status == TaskStatus.WARNING.value:
            self.status_msg = 'warning|' + self.status_msg
            self.last_warning_time = datetime.now()
        else:
            for strategy_id in self.strategies:
                self.strategies[strategy_id].status = status
        self.send_status()

    def error_handler(self, status, msg):
        self.status = status
        self.status_msg = msg
        self.on_finish()

    def send_request(self, req, rtype='PDT', channel=PublishChannel.PDT.value):
        """
        push request to requests queue;
        params reference process_requests in driver.py
        :param req: request [pub channel, pub value]
        :param rtype: request type
        :param channel: channel in pdt/ui
        """
        self.requests.append({
            'rtype': rtype,
            'channel': channel,
            'request': req
        })

    def send_order(self, exchange=None, symbol=None, contract_type=None, price=None, quantity=None, direction=None,
                   order_type=None, account_id=None, strategy_key=None, delay=None, post_only=False, strategy_id=None):
        """
        send order
        :param strategy_id: string strategy_id|'SAMPLE_Binance_BTCUSDT_20190725152929'
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
        order = {
            "exchange": exchange,
            "symbol": symbol,
            "account_type": AccountType.EXCHANGE.value,
            "contract_type": contract_type,
            "price": price,
            "quantity": quantity,
            "direction": direction,
            "order_type": order_type,
            "account_id": account_id,
            "strategy_key": strategy_key,
            "delay": delay if delay is not None else 59000,
            "post_only": post_only,
            "filled": 0,
            "avg_price": 0,
            "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # notes 字段会以json写入数据库
            "notes": {"task_id": self.task_id, "strategy_id": strategy_id}
        }

        self.order_count += 1
        request = {
            'strategy': self.strategy_name,
            'task_id': self.task_id,
            "strategy_id": strategy_id,
            'ref_id': f'{time.strftime("%Y%m%d%H%M%S", time.localtime())}_{self.order_count:08}',
            'action': OrderActions.SEND.value,
            'metadata': order
        }
        order_info = f"{order['account_id']} {order['strategy_key']} {order['exchange']} {order['symbol']} " \
                     f"{order['order_type']} {order['direction']} {order['quantity']}@{order['price']}"
        logger.info(f"SendOrder => {strategy_id} {request['ref_id']} {order_info} {request['strategy']} {request['task_id']}")
        # 发单的时候增加资金占用量
        _, base, quote = self.get_base_quote_name(symbol)
        increase_reserved_amount(self.balance_by_order_res[f"{exchange}|{account_id}"], base, quote, direction, quantity, price)
        self.orders[strategy_id][request['ref_id']] = order
        self.pending_orders[strategy_id][request['ref_id']] = order
        self.send_request([self.trade_request_key, request])

    def on_response_process(self, response):
        """
        handle order_info received from pdt
        :param response: {
            ref_id: request.ref_id,
            action: request.action,
            strategy: request.strategy,
            metadata: metadata
        }
        """
        response = json.loads(response['data'])

        # 其他非发单相关的回报, 直接丢掉就好
        if 'request' not in response['metadata'] or 'task_id' not in response['metadata']['request']:
            return
        # 如果回报不属于自己, 直接丢掉就好
        if response['strategy'] != self.strategy_name or response['metadata']['request']['task_id'] != self.task_id:
            return

        try:
            logger.file(f'OriginalResponse => {json.dumps(response)}')
            strategy_id = response['metadata']['request']['strategy_id']
            order_response = {
                'ref_id': response['ref_id'],
                'strategy_id': strategy_id,
                'action': response['action'],
                'task_id': response['metadata']['request']['task_id'],
                'exchange': response['metadata']['exchange'],
                'account_id': response['metadata']['metadata']['account_id'],
                'symbol': response['metadata']['symbol'],
                'contract_type': response['metadata']['contract_type'],
                'timestamp': response['metadata']['timestamp'],
                'status': None,
                'direction': Direction.BUY.value,
                'original_amount': 0,
                'original_price': 0,
                'filled': 0,
                'avg_executed_price': 0
            }
            order_info = response['metadata']['metadata']
            origin_order = self.orders[strategy_id][order_response['ref_id']]
            self.origin_order_update_response(order_response, origin_order)
            if response['metadata']['event'] == RequestActions.SEND_ORDER.value:
                if order_response['ref_id'] not in self.pending_orders[strategy_id]:  # 重复推送无效回报, 直接退出处理
                    return

                if order_info['result'] is False:
                    self.pdt_error_handler(response, strategy_id)
                    logger.error(f"Send order fail => {strategy_id} {order_response['ref_id']} "
                                 f"error_code: {order_info['error_code']} error_msg: {order_info['error_code_msg']}")
                    order_response['status'] = OrderStatus.REJECTED.value
                else:  # 发单成功, 收到交易所回报
                    if order_response['exchange'] in ORDER_UPDATE_EX and ORDER_UPDATE_EX[order_response['exchange']]:
                        ex_sy_order_id = f"{order_response['exchange']}|{order_response['symbol']}|{order_info['order_id']}"
                        self.strategy_order_map[ex_sy_order_id] = {
                            'strategy_id': strategy_id,
                            "ref_id": response['ref_id'],
                        }
                    order_response['order_id'] = order_info['order_id']
                    order_response['status'] = OrderStatus.PENDING.value
                self.on_response(order_response)

            if response['metadata']['event'] == RequestActions.CANCEL_ORDER.value:
                if order_response['ref_id'] not in self.active_orders[strategy_id]:  # 重复推送无效回报, 直接退出处理
                    return

                if order_info['result'] is False:  # 撤单失败, 说明订单已经处于终结状态, 无需重复处理
                    # 重置pending_cancel标记, 可以下次再撤单
                    origin_order['pending_cancel'] = False
                    # TODO 撤单失败的多种原因
                    self.pdt_error_handler(response, strategy_id)
                    self.inspect_order(strategy_id, response['ref_id'])
                    return

                # order_response['status'] = OrderStatus.CANCELLED.value
                # self.on_response(order_response)
                # 撤单回报中没有已经成交的量, 所以需要再inspect一次
                self.inspect_order(strategy_id, order_response['ref_id'])

            if response['metadata']['event'] == RequestActions.INSPECT_ORDER.value:
                if order_response['ref_id'] not in self.active_orders[strategy_id]:  # 不是fak订单, 单纯的查询
                    return

                if order_info['result'] is False:
                    # 查询失败, 无需任何操作
                    if str(order_info['error_code'][3:]) != '535' or response['metadata']['exchange'] == 'Bitflyer':
                        self.pdt_error_handler(response, strategy_id)
                        return
                    # 有些交易所订单已撤销，但是会返回不存在错误，需要特殊处理
                    detail_order_info = response['metadata']['order_info']
                    detail_order_info['status'] = OrderStatus.CANCELLED.value
                    if detail_order_info['original_amount'] == 'unknown' or detail_order_info['filled'] == 'unknown':
                        detail_order_info['original_amount'] = origin_order['quantity']
                        detail_order_info['filled'] = origin_order['filled']
                        detail_order_info['avg_executed_price'] = origin_order['avg_price']

                self.inspect_update_response(order_response, response)
                self.on_response(order_response)

            # 更新订单update_time
            origin_order['update_time'] = get_datetime(order_response['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

            # 打印目前的订单状态
            # self.debug_print_orders()
        except Exception as e:
            logger.error(e)
            sentry.captureException()

    @staticmethod
    def origin_order_update_response(order_response, origin_order):
        """
        update order info from pdt in direction, quantity, price
        :param order_response: reference order_response in on_response_process
        :param origin_order: order info from pdt
        :return:
        """

        order_response['direction'] = origin_order['direction']
        order_response['original_amount'] = origin_order['quantity']
        order_response['original_price'] = origin_order['price']

    @staticmethod
    def inspect_update_response(order_response, response):
        """
        update order info from pdt(order action: inspect)
        """
        order_info = response['metadata']['order_info']

        amount = order_info['original_amount']
        filled = order_info['filled'] if order_info['filled'] is not None else 0
        avg_price = order_info['avg_executed_price'] if order_info['avg_executed_price'] is not None else 0

        order_response['original_amount'] = float(amount) if isinstance(amount, str) else amount
        order_response['filled'] = float(filled) if isinstance(filled, str) else filled
        order_response['avg_executed_price'] = float(avg_price) if isinstance(avg_price, str) else avg_price
        # 直接使用PDT的状态信息
        order_response['status'] = order_info['status']

    def on_response(self, response):
        """
        这里已经整合了目前的回报信息, 提供统一的格式
        response = {
            'strategy_id': 0,
            'ref_id': 'uuid',
            'action': OrderActions.SEND.value,
            'task_id': 'ICEBERG_Binance_BTCUSDT_20190725152929',
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
        # 基类统一的balance管理
        self.balance_management(response)

        # 调用对应策略的on_response函数
        self.strategies[response['strategy_id']].on_response(response)

        # 基类统一的订单管理
        self.order_management(response)

    def on_balance_response(self, balance):
        """
        {
            "exchange":"Coinflex",
            "account_id":"tradingtwo",
            "metadata":{
                "exchange":"Coinflex",
                "posInfoType":"spot_position",
                "metadata":{
                    "account_id":"tradingtwo",
                    "result":false,
                    "error_code":999999,
                    "error_code_msg":"Coinflex does not support spot position trading at present",
                    "timestamp":"20191031160422530"
                },
                "timestamp":"20191031160422530"
            },
            "global_balances":{
                "spot_balance":{
                    "FLEX":{
                        "available":702687.1225,
                        "reserved":0,
                        "shortable":0,
                        "total":702687.1225
                    },
                    "result":true,
                    "account_id":"tradingtwo",
                    "timestamp":"20191031160412897"
                }
            },
        }
        """
        # 获取定时的balance推送
        try:
            balance = balance['data']
            data = json.loads(balance)
            if data['exchange'] in self.valid_exchanges and data['account_id'] in self.valid_account_id:
                if 'spot_balance' in data['global_balances']:
                    spot_balance = data['global_balances']['spot_balance']
                    if 'result' in spot_balance and spot_balance['result'] is False:
                        self.pdt_error_handler(data, None)
                        return
                    exch_acc = f"{data['exchange']}|{data['account_id']}"
                    self.balance[exch_acc] = data['global_balances']['spot_balance']
                    self.balance_status[exch_acc] = True
        except Exception as e:
            logger.error(e)
            sentry.captureException()

    def pdt_error_handler(self, response, strategy_id):
        """
        handle error of pdt
        :param strategy_id:
        :param response: pdt response
        """
        self.error_count += 1
        if 'action' in response:
            error_code = response['metadata']['metadata']['error_code']
            error_code_msg = response['metadata']['metadata']['error_code_msg']
        else:
            error_code = response['global_balances']['spot_balance']['error_code']
            error_code_msg = response['global_balances']['spot_balance']['error_code_msg']
        if error_code == 999999:
            error_code = '999999'
            error_code_msg = '错误码暂未定义 ' + error_code_msg
        elif error_code[3:] in ('105', '106'):
            error_code_msg = '下单量太小 ' + error_code_msg
        elif error_code[3:] in ('109', '110'):
            error_code_msg = '下单价格或数量超出限制 ' + error_code_msg
        elif error_code[3:] in ('500', '501', '503', '508', '509'):
            error_code_msg = '系统错误 ' + error_code_msg
        elif error_code[3:] == '502':
            if response['action'] == RequestActions.INSPECT_ORDER.value:
                self.strategies[strategy_id].process_frequency_error()
                error_code_msg = '用户请求频率过快 ' + error_code_msg
        else:
            error_code = '999999'
            error_code_msg = '错误码暂未定义 ' + error_code_msg
        self.update_status(TaskStatus.WARNING.value, error_code_msg)
        msg_show = f'ERROR => {error_code} {error_code_msg} {json.dumps(response)}'
        logger.error(msg_show)
        if self.error_count == 5:  # 连续5次发现异常上报sentry
            sentry.captureMessage(msg_show)
            self.error_count = 0

    def balance_management(self, response):
        """
        balance management, update balance_by_order_res from order info
        """
        strategy_id = response['strategy_id']
        ref_id = response['ref_id']

        # 如果订单已经处于完成状态, 无需重复处理, 直接跳过
        if ref_id in self.finished_orders[strategy_id]:
            return

        _, base, quote = self.get_base_quote_name(response["symbol"])
        origin_order = self.orders[strategy_id][ref_id]
        ex_acc = f"{response['exchange']}|{response['account_id']}"

        ret = balance_management_common_process(self.balance_by_order_res[ex_acc], response, base, quote, origin_order)
        if ret:
            self.balance_status[ex_acc] = True
            logger.debug("Master Balance => ", json.dumps(self.balance_by_order_res))

    def order_management(self, response):
        """
        order management, update active_orders、finished_orders
        :param response: reference order_response in on_response_process
        """
        strategy_id = response['strategy_id']
        ref_id = response['ref_id']
        if response['status'] == OrderStatus.REJECTED.value:  # 发单失败, 直接在pending删除对应订单
            origin_order = self.pending_orders[strategy_id].pop(ref_id, None)
            if origin_order is not None:
                origin_order['status'] = OrderStatus.REJECTED.value
                self.finished_orders[strategy_id][ref_id] = origin_order

        elif response['status'] == OrderStatus.PENDING.value:  # 发单成功, 收到交易所回报, 添加订单信息, 转移到active_orders
            origin_order = self.pending_orders[strategy_id].pop(ref_id, None)
            if origin_order is not None:
                origin_order['order_id'] = response['order_id']
                origin_order['account_id'] = response['account_id']
                origin_order['status'] = OrderStatus.PENDING.value
                self.active_orders[strategy_id][ref_id] = origin_order

        elif response['status'] == OrderStatus.CANCELLED.value:  # 撤单成功, 转移订单到finished_orders
            origin_order = self.active_orders[strategy_id].pop(ref_id, None)
            if origin_order is not None:
                if response["filled"] > 0:
                    origin_order['filled'] = response["filled"]
                    origin_order['avg_price'] = response["avg_executed_price"]
                origin_order['status'] = OrderStatus.CANCELLED.value
                self.finished_orders[strategy_id][ref_id] = origin_order

        elif response['status'] == OrderStatus.PARTIALLY_FILLED.value:  # 部分成交, 若为fak单, 需要转移到active_orders
            if ref_id in self.active_orders[strategy_id]:
                origin_order = self.active_orders[strategy_id][ref_id]
                if response["filled"] > 0:
                    origin_order['filled'] = response["filled"]
                    origin_order['avg_price'] = response["avg_executed_price"]
                origin_order['status'] = OrderStatus.PARTIALLY_FILLED.value

        elif response['status'] == OrderStatus.FILLED.value:  # 完全成交, 转移订单到finished_orders
            if ref_id in self.active_orders[strategy_id]:
                origin_order = self.active_orders[strategy_id].pop(ref_id)
                if response["filled"] > 0:
                    origin_order['filled'] = response["filled"]
                    origin_order['avg_price'] = response["avg_executed_price"]
                origin_order['status'] = OrderStatus.FILLED.value
                self.finished_orders[strategy_id][ref_id] = origin_order

    def get_request(self):
        """
        get requests of algo
        :return: requests of algo; type: list;
        """
        return self.requests

    def clear_request(self):
        """
        set requests to blank list
        """
        self.requests = []

    def cancel_order(self, strategy_id, ref_id, force_cancel=False):
        """
        cancel order by ref_id, order info is stored in active_orders, key is ref_id
        :param strategy_id: type string sub strategy id
        :param ref_id: type string order ref_id
        :param force_cancel type bool 强行撤单, 无视pending_cancel标记
        :return:
        """
        if ref_id not in self.active_orders[strategy_id]:
            logger.error('cancel_order order ref_id error: ', ref_id)
            return

        origin_order = self.active_orders[strategy_id][ref_id]
        # 对于不是强行撤单的单, 已经发过撤单信号, 无需再发撤单
        if not force_cancel and 'pending_cancel' in origin_order and origin_order['pending_cancel']:
            return
        origin_order['pending_cancel'] = True
        request = {
            'strategy': self.strategy_name,
            'task_id': self.task_id,
            'strategy_id': strategy_id,
            'ref_id': ref_id,
            'action': OrderActions.CANCEL.value,
            'metadata': {
                'exchange': origin_order['exchange'],
                'symbol': origin_order['symbol'],
                'order_id': origin_order['order_id'],
                'contract_type': origin_order['contract_type'],
                'account_id': origin_order['account_id'],
                'direction': origin_order['direction'],
                'strategy_key': origin_order['strategy_key'],
                'price': origin_order['price'],
                'quantity': origin_order['quantity']
            }
        }
        order_info = f"{origin_order['account_id']} {origin_order['strategy_key']} {origin_order['exchange']} " \
                     f"{origin_order['symbol']} {origin_order['direction']} {origin_order['quantity']}@{origin_order['price']}"
        logger.info(f"CancelOrder => {strategy_id} {request['ref_id']} {order_info} {request['strategy']} {request['task_id']}")
        self.send_request([self.trade_request_key, request])

    def inspect_order(self, strategy_id, ref_id):
        """
        inspect order by ref_id, order info is stored in active_orders ,key is ref_id
        :param strategy_id: type string
        :param ref_id: type string
        """

        if ref_id not in self.active_orders[strategy_id]:
            logger.error('inspect_order order ref_id error: ', ref_id)
            return

        origin_order = self.active_orders[strategy_id][ref_id]
        request = {
            'strategy': self.strategy_name,
            'task_id': self.task_id,
            'ref_id': ref_id,
            'strategy_id': strategy_id,
            'action': OrderActions.INSPECT.value,
            'metadata': {
                'exchange': origin_order['exchange'],
                'symbol': origin_order['symbol'],
                'order_id': origin_order['order_id'],
                'contract_type': origin_order['contract_type'],
                'account_id': origin_order['account_id'],
                'direction': origin_order['direction'],
                'strategy_key': origin_order['strategy_key']
            }
        }
        self.send_request([self.trade_request_key, request])

    def get_base_quote_name(self, symbol):
        """
        get base, quote info by symbol
        :param symbol: symbol
        :return: [symbol, base, quote]
        """
        return self.valid_symbols[symbol]

    def send_status(self):
        """
        send algo status to UI
        """
        if self.status_msg.split('|')[0] == TaskStatus.WARNING.value and \
                (datetime.now() - self.last_warning_time).total_seconds() > 10 * 60:
            self.status_msg = '任务正在运行'

        status_obj = {
            'ip': self.ip,
            'pid': self.pid,
            'name': self.task_id,
            'status': self.status,
            'status_msg': self.status_msg,
            'start_time': self.task['start_time'],
            'end_time': self.task['end_time'],
            'update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:-3],
        }

        strategy_status = {}
        for strategy_id in self.strategies:
            st_task = self.task['strategies'][strategy_id]
            strat = self.strategies[strategy_id]
            initial_balance = st_task['initial_balance']
            exch_acc = f"{st_task['exchange']}|{st_task['account']}"
            if self.balance_status[exch_acc]:
                _, base, quote = st_task["symbol"]
                if st_task['exchange'] in BALANCE_BY_ORDER_RES_EX and BALANCE_BY_ORDER_RES_EX[st_task['exchange']]:
                    base_currency = get_total_balance(strat.balance, base)
                    quote_currency = get_total_balance(strat.balance, quote)
                else:
                    base_currency = get_total_balance(self.balance[exch_acc], base)
                    quote_currency = get_total_balance(self.balance[exch_acc], quote)
                factor = 1 if st_task['direction'] == Direction.SELL.value else -1
                if st_task["currency_type"] == CurrencyType.BASE.value:
                    strat.deal_size = initial_balance[base] - base_currency
                else:
                    strat.deal_size = quote_currency - initial_balance[quote]
                strat.deal_size *= factor

            strat.deal_size = round(strat.deal_size, get_formated_decimal_from_number(strat.deal_size, DEAL_SIZE_MAX_DISPLAY, True))

            strategy_status[strategy_id] = {
                'strategy_id': st_task['strategy_id'],
                'exchange': st_task['exchange'],
                'account': st_task['account'],
                'symbol': st_task['symbol'][0],
                'direction': st_task['direction'],
                'currency_type': st_task['currency_type'],
                'price_threshold': st_task['price_threshold'],
                'total_size': st_task['total_size'],
                'start_time': st_task['start_time'],
                'end_time': st_task['end_time'],
                'deal_size': strat.deal_size,
                'attention': strat.attention,
                'current_price': strat.current_price,
                'status': strat.status,
                'status_msg': strat.status_msg,
            }
        status_obj['strategies'] = strategy_status
        self.send_request([self.config['REDIS_TASK_STATUS'], status_obj], rtype='Status', channel=PublishChannel.UI.value)

    def cal_current_price(self, market_data):
        asks = market_data['metadata']['asks']
        bids = market_data['metadata']['bids']
        for strategy_id in self.strategies:
            st_task = self.task['strategies'][strategy_id]
            strat = self.strategies[strategy_id]

            if st_task['algorithm'] in [Algorithms.SAMPLE.value, Algorithms.ICEBERG.value, Algorithms.VWAP.value, Algorithms.TWAP.value]:
                if market_data['symbol'] == st_task['symbol'][0]:
                    strat.current_price = asks[0][0] if st_task['direction'] == Direction.BUY.value else bids[0][0]

            if 'median' in st_task and market_data['symbol'] == st_task['median'][0]:
                if 'anchor_price' not in st_task or not st_task['anchor_price']:
                    # algo use price_threshold, didn't need to compute anchor price
                    strat.current_price = asks[0][0] if st_task['direction'] == Direction.BUY.value else bids[0][0]

                if strat.anchor_price <= 0:
                    # Didn't receive anchor price yet
                    strat.current_price = None

                median_price = format_price((asks[0][0] + bids[0][0]) / 2, get_price_precision(st_task, '', st_task['median'][0]))
                if st_task['median'][2] == st_task['symbol'][2]:
                    strat.current_price = round(median_price * strat.anchor_price, 8)
                else:
                    strat.current_price = round(median_price / strat.anchor_price, 8)

            if 'anchor' in st_task and market_data['symbol'] == st_task['anchor'][0]:
                strat.anchor_price = format_price((asks[0][0] + bids[0][0]) / 2, get_price_precision(st_task, '', st_task['anchor'][0]))

    def save_all_order_info(self):
        all_order_info = {
            'pending_orders': self.pending_orders,
            'active_orders': self.active_orders,
            'finished_orders': self.finished_orders,
        }
        save_orders(all_order_info, f'{self.task["task_id"]}.json')

    def inspect_order_on_time(self):
        """
        timely inspect all active orders of algo
        """
        for strategy_id in self.strategies:
            self.strategies[strategy_id].inspect_order_on_time()

    def cancel_all_order(self):
        """
        cancel all orders of algo
        """
        # TODO 之后考虑一下频率控制
        for strategy_id in self.strategies:
            for ref_id in self.active_orders[strategy_id]:
                self.cancel_order(strategy_id, ref_id, True)

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
        msg = f'Alarm------------- {alarm_code} {alarm_msg}'
        logger.warning(msg)
        if self.task['test_mode'] or not self.task['alarm']:
            # no need to send alarm under test env
            return
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.send_request(['MM:strategy_alarm', {
            'strategy_name': self.strategy_name,
            'code': alarm_code,
            'msg': f'{cur_time}: {self.strategy_name} {self.task_id} {alarm_msg}'
        }], rtype='Alarm', channel=PublishChannel.ALARM.value)

    def debug_print_orders(self):
        """
        help to debug order management, using pending、active、finishing orders
        :return:
        """
        for strategy_id in self.strategies:
            logger.file('pending orders -------------------')
            for ref_id in self.pending_orders[strategy_id]:
                logger.file(ref_id + ' ' + json.dumps(self.pending_orders[strategy_id][ref_id]))
            logger.file('\n')

            logger.file('active orders -------------------')
            for ref_id in self.active_orders[strategy_id]:
                logger.file(ref_id + ' ' + json.dumps(self.active_orders[strategy_id][ref_id]))
            logger.file('\n')

            logger.file('finished orders -------------------')
            for ref_id in self.finished_orders[strategy_id]:
                logger.file(ref_id + ' ' + json.dumps(self.finished_orders[strategy_id][ref_id]))
            logger.file('\n')

    def on_send_order_response(self, send_order_response):
        """
        for "limit" and "market" orders:
        response.metadata = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: "spot",
            event: "place_order",
            metadata: {
                result: false,
                error_code: 123,
                error_code_msg: "各种error原因"
                account_id: "act_test",
                order_id: "123456"
            },
            timestamp: YYYYMMDDHHmmssSSS
        }

        for "fak" orders:
        response.metadata = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: "spot",
            event: "inspect_order",
            metadata: {
                result: true,
                cancel_result: true,
                inspect_result: true,
                account_id: "act_test",
                order_id: "123456"
            },
            order_info: {
                original_amount: 1.0,
                filled: 1.0,
                status: "filled",
                avg_executed_price: 100.0
        },
        timestamp: YYYYMMDDHHmmssSSS
        }
        """
        pass

    def on_cancel_order_response(self, cancel_order_response):
        """
        cancel_order_response.metadata = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: "spot",
            event: "cancel_order",
            metadata: {
                result: true,
                account_id: "act_test",
                order_id: "123456"
            },
            timestamp: YYYYMMDDHHmmssSSS
        }
        """
        pass

    def on_inspect_order_response(self, inspect_order_response):
        """
        response.metadata = {
            exchange: "Bitfinex",
            symbol: "BTCUSD",
            contract_type: "spot",
            event: "inspect_order",
            metadata: {
                result: true,
                account_id: "act_test",
                order_id: "123456"
            },
            order_info: {
                original_amount: 1.0,
                filled: 1.0,
                status: "filled",
                avg_executed_price: 100.0
            },
            timestamp: YYYYMMDDHHmmssSSS
        }
        """
        pass

    def clear_timeout_pending_orders(self, strategy_id, clean_pending=False):
        pending_orders = self.pending_orders[strategy_id]
        if clean_pending:
            for ref_id in list(pending_orders.keys()):
                pending_orders.pop(ref_id)
        else:
            for ref_id in list(pending_orders.keys()):
                if (datetime.now() - str_to_datetime(pending_orders[ref_id]['create_time'])).total_seconds() > 10 * 60:
                    pending_orders.pop(ref_id)
                        
    def get_trade_orders(self):
        trade_orders = copy.deepcopy(self.finished_orders)
        for strategy_id in self.active_orders:
            if strategy_id in self.finished_orders:
                trade_orders[strategy_id].update(self.active_orders[strategy_id])
            else:
                trade_orders[strategy_id] = self.active_orders[strategy_id]
        return trade_orders

    def count_unfinished_order(self):
        count = 0
        for strategy_id in self.pending_orders:
            count += len(self.pending_orders[strategy_id])
        for strategy_id in self.active_orders:
            count += len(self.active_orders[strategy_id])
        return count

    def check_trade_precision(self, market_data):
        price_precision = self.task['coin_config'][market_data['exchange']][market_data['symbol']]['price_precision']
        ask0 = market_data['metadata']['asks'][0][0]
        if not check_precison_of_number(ask0, price_precision):
            self.alarm(f'{market_data["exchange"]} {market_data["symbol"]} price precision error! Ask0: {ask0}, price precision: {price_precision}', AlarmCode.EXECUTE_ABNORMAL.value)
            self.update_status(TaskStatus.WARNING.value, f'{market_data["symbol"]} price precision error!')
            return False
        return True
