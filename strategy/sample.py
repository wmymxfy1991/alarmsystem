# encoding: utf-8

from util.logger import logger
from strategy.strategy_base import StrategyBase


class Sample(StrategyBase):
    def __init__(self):
        super().__init__()

    def on_init(self, config, task, master_ptr):
        super().on_init(config, task, master_ptr)

    def on_book(self, market_data):
        # 调用公用的行情处理
        super().on_book(market_data)

    def on_orderbook_ready(self, orderbook):
        print(orderbook)

    def on_response(self, response):
        super().on_response(response)

    def on_timer(self):
        super().on_timer()

    def on_finish(self):
        super().on_finish()
