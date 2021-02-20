# encoding: utf-8
import asyncio
import json
import signal
import os
import platform
import sys
from datetime import datetime

from config.config import *
from config.enums import *
from util.alioss import alioss
from util.aredis import RedisHandler
from util.logger import logger
from util.util import get_ip, get_pid, get_git_msg
from strategy.strategy_master import StrategyMaster

task_mock = {
    "task_id": "XXXXXXXXX",  # 全局唯一标记, 最后一个为精度为秒的时间戳
    "initial_balance": {  # 账户中初始的币的数量
        "Binance|trader1": {
            "BTC": 100.12,
            "USDT": 12.21
        }
    },
    # "eaas_mode": "Single",  # eaas模式, 可选 Single/Multiple, 选Single的时候为传统的单策略模式
    "strategies": {
        "SAMPLE_Binance_BTCUSDT_20190725152929": {
            "algorithm": "SAMPLE",  # 算法名称, 目前可选ICEBERG/TWAP/SAMPLE
            "exchange": "Binance",
            "account": "trader1",

            "direction": "Buy",  # 买卖方向, 可选Buy/Sell
            "currency_type": "Base",  # 可选Base/Quote, 以BTCUSDT为例, 选Base时执行总量按BTC的量来算
            "total_size": 1000,  # 总需要成交交易量，当 token 的 balance 变化超过（买）或低于（卖）此量，算法认为交易完成

            "symbol": ["BTCUSDT", "BTC", "USDT"],
            # "median": ["BTCEOS", "BTC", "EOS"],
            # "anchor": ["EOSUSDT", "EOS", "USDT"],

            "price_threshold": None,  # 发单的价格限制, 高于该价格不发单
            "anchor_price": None,
            "transfer_coin": False,  # 是否转换货币
            "execution_mode": "Passive",  # 消极执行, 目前可选Passive/Aggressive
            "exchange_fee": 0.001,  # 交易所对每笔交易收取的手续费

            "start_time": "2019-06-28 00:00:00",  # 可选项, 如果不填的话使用全局值
            "end_time": "2019-06-29 00:00:00",  # 可选项, 如果不填的话使用全局值
            "trade_role": "Taker",  # 可选项, 如果不填的话使用全局值, 四个选择 Maker/Taker/Both/None
        }
    },
    "end_time": "2019-06-29 00:00:00",
    "start_time": "2019-06-28 00:00:00",
    "coin_config": {
        # 有些交易所最小发单单位以base来计, 有些以quote来计, 只用填一个, 另外一个为0
        "Binance": {
            "BTCUSDT": {
                "base_min_order_size": 0.01,
                "quote_min_order_size": 0,
                "price_precision": 0.001,
                "size_precision": 0.01
            }
        }
    },
    "trade_role": "Taker",  # 四个选择 Maker/Taker/Both/None
    "customer_id": "amberai",
    "alarm": True,  # 对外发布告警
    "test_mode": True,  # 测试模式, 下单API为模拟返回, 随机成交模式
    "local_debug": False  # 适合用pycharm本地调试, 不使用OSS
}


class Driver:
    def __init__(self):
        self.task = {}
        self.config = CONFIG_GLOBAL
        self.r_ui, self.p_ui = RedisHandler().connect(CONFIG_GLOBAL['REDIS_UI'])
        self.r_pdt, self.p_pdt = RedisHandler().connect(CONFIG_GLOBAL['REDIS_PDT'])
        self.r_alarm, self.p_alarm = RedisHandler().connect(CONFIG_GLOBAL['REDIS_ALARM'])
        self.strategy_master = None  # strategy_master instance
        self.redis_monitor = ''  # used to record latest push info to ui
        self.execute_time = datetime.now()  # algo on_timer invoke time, init set to now()
        # 注册程序关闭事件
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self.signal_handler)

        if platform.system() == "Linux":
            signal.signal(signal.SIGHUP, self.signal_handler)

    def signal_handler(self, signum, frame):
        """
        algo exit when receive abnormal signal of linux
        :param signum: type: int
        :param frame: not used
        """
        exit_msg = f'Received abort signal({signum}) and exit success'
        logger.error(exit_msg)
        logger.flush()
        if self.strategy_master is not None:
            self.strategy_master.error_handler(TaskStatus.ERROR.value, '程序意外终止 ' + exit_msg)
        else:
            sys.exit(0)

    async def process_request(self, req):
        """
        request handle
        :param req:{
           rtype: request type, in Subscribe、Alarm、Status、Exit、Publish(default)
           channel:　pdt/ui, use for diff redis
           request:{
              keyword: redis publish channel
              body: redis publish data, must be str format(json.dumps(body)) in python
           }
        }
        """
        rtype = req['rtype']
        channel = req['channel']
        keyword, body = req['request']
        logger.file(f'ProcessRequest => rtype: {rtype} channel: {channel} key: {keyword} body: {body}')
        if rtype == 'Subscribe':
            # 策略初始化结束后开始执行redis订阅
            await self.p_pdt.subscribe(**body)
            await self.p_ui.subscribe(**{CONFIG_GLOBAL['REDIS_TASK_COMMAND']: self.strategy_master.on_command})
            return

        if rtype == 'Status':
            if body['status'] == TaskStatus.ERROR.value:
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_NOTIFICATION'], json.dumps({
                    'type': body['status'],
                    'message': body["name"],
                    'description': f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {body["status_msg"]}'
                }))
            await self.r_ui.rpush(CONFIG_GLOBAL['REDIS_TASK_STATUS'], json.dumps(body))
            body['task'] = self.strategy_master.task
            await self.r_ui.hset(self.redis_monitor, self.task["task_id"], json.dumps(body))
            return

        if not isinstance(body, str):
            body = json.dumps(body)

        if rtype == 'Alarm':
            # Alarm使用了不同的redis
            await self.r_alarm.publish(keyword, body)
            return
        if rtype == 'Exit':
            logger.flush()
            sys.exit(0)

        if channel == PublishChannel.PDT.value:
            await self.r_pdt.publish(keyword, body)
        elif channel == PublishChannel.UI.value:
            await self.r_ui.publish(keyword, body)

    async def main_process(self):
        """
        main process, listen data from pdt/ui, invoke on_timer of algo
        """
        # 在任务队列里抢单, 抢到之后开始执行
        task = await self.r_ui.blpop(CONFIG_GLOBAL['REDIS_ADD_TASK_QUEUE'])
        self.task = json.loads(task[1])
        # self.task = task_mock
        self.redis_monitor = f'{"test_" if self.task["test_mode"] else ""}{CONFIG_GLOBAL["REDIS_STATUS_MONITOR"]}'
        # 接收到task之后初始化阿里云OSS
        local_debug = False
        if 'local_debug' in self.task and self.task['local_debug']:
            local_debug = True

        alioss.init(local_debug)
        try:
            log_name = f'{"TEST_" if self.task["test_mode"] else "EAAS_"}{self.task["task_id"]}.txt'
            logger.init(log_name, local_debug)
            logger.debug("====================START LINE====================")
            logger.debug(f'IP: {get_ip()} PID: {get_pid()} GIT: {get_git_msg()}')

            # 拿到无状态的任务时, 开始初始化策略
            self.strategy_master = StrategyMaster()
            self.strategy_master.on_init(CONFIG_GLOBAL, self.task)
        except Exception as e:
            logger.error(e)
            sentry.captureException()
            self.strategy_master.error_handler(TaskStatus.ERROR.value, '程序初始化意外终止')
        finally:
            logger.flush()

        while True:
            try:
                # 收集策略发出的request, 并且顺序推送至PDT
                requests = self.strategy_master.get_request()
                if len(requests) > 0:
                    for req in requests:
                        await self.process_request(req)
                    self.strategy_master.clear_request()
                # 异步监听redis频道, 往对应的策略handler中推送
                await self.p_pdt.get_message(timeout=0.1)
                await self.p_ui.get_message(timeout=0.0001)
                await asyncio.sleep(0.001)

                # 策略定时执行函数, 默认3秒一次
                cur_time = datetime.now()
                if (cur_time - self.execute_time).total_seconds() > CONFIG_GLOBAL["TIME_INTERVAL"]:
                    self.execute_time = cur_time
                    self.strategy_master.on_timer()
                    logger.flush()
            except Exception as e:
                logger.error(e)
                logger.flush()
                sentry.captureException()

    def run(self):
        """
        start of eaas
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_process())


if __name__ == '__main__':
    driver_instance = Driver()
    driver_instance.run()
