# encoding: utf-8
import json
import time
from datetime import datetime

from aredis import StrictRedis
import asyncio
import pandas as pd

from config.enums import *
from config.config import CONFIG_GLOBAL
from util.logger import *
from util.aredis import RedisHandler


class Toolkit:
    def __init__(self, exchange, symbol, account):
        self.strategy_name = 'Execution'
        self.exchange = exchange
        self.symbol = symbol
        self.account = account
        self.market_data = {}
        self.requests = []
        self.last_kline_timestamp = 0
        self.last_kline_vol = 0
        self.market_cum_vol = 0
        self.orderbook_cocunter = 0
        self.trade_request_key = IntercomScope.TRADE.value + ':' + self.strategy_name + '_request'
        self.r_pdt, self.p_pdt = RedisHandler().connect(CONFIG_GLOBAL['REDIS_PDT'])
        self.register()

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_process())

    async def main_process(self):
        market_key = '|'.join([self.exchange, self.symbol, 'spot', 'orderbook', '20'])
        # market_key = '|'.join([self.exchange, self.symbol, 'spot', 'kline', '|1m'])
        print('market_key:', market_key)
        market_response_key = IntercomScope.MARKET.value + ':' + market_key
        trade_response_key = IntercomScope.TRADE.value + ':' + self.strategy_name + '_response'
        await self.p_pdt.subscribe(**{market_response_key: self.on_book})
        await self.p_pdt.subscribe(**{trade_response_key: self.on_response})
        # toolkit_instance.get_balance()
        # toolkit_instance.send_order('0.0000001976', '2000', Direction.BUY.value)
        while True:
            if len(self.requests) > 0:
                for req in self.requests:
                    await self.process_request(req)
                self.requests = []

            await self.p_pdt.get_message(timeout=1)
            await asyncio.sleep(0.001)

    async def process_request(self, req):
        if isinstance(req, list):
            keyword, body, rtype = req
        elif isinstance(req, dict):
            rtype = req['rtype']
            print('request: ', req['request'])
            keyword, body, rtype = req['request']

        if rtype == PublishChannel.PDT.value:
            print('req: ', req)
            await self.r_pdt.publish(keyword, body)
        elif rtype == PublishChannel.UI.value:
            await self.r_ui.publish(keyword, body)

    def register(self):
        market_key = '|'.join([self.exchange, self.symbol, 'spot', 'orderbook', '20'])
        # market_key = '|'.join([self.exchange, self.symbol, 'spot', 'kline', '|1m'])

        market_response_key = IntercomScope.MARKET.value + ':' + market_key
        # balance not used now
        # balance_request_key = IntercomScope.POSITION.value + ':Poll Position Request'
        # balance_response_key = IntercomScope.POSITION.value + ':' + self.exchange + '|' + self.account
        # await self.p_pdt.subscribe(**{balance_response_key, self.on_balance})

        self.send_request(
            [IntercomScope.MARKET.value + ':' + IntercomChannel.SUBSCRIBE_REQUEST.value, json.dumps(market_key),
             PublishChannel.PDT.value])

    def get_balance(self):
        metadata = {
            "exchange": self.exchange,
            'account_id': self.account,
            'currency': ''
        }
        request = {
            'strategy': self.strategy_name,
            'ref_id': 'test',
            'action': RequestActions.QUERY_BALANCE.value,
            'metadata': metadata
        }
        self.requests.append(
            [IntercomScope.TRADE.value + ':eaas_execution' + '_request', json.dumps(request), PublishChannel.PDT.value])

    def get_active_order(self):
        metadata = {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'contract_type': 'spot',
            'account_id': self.account
        }
        request = {
            'strategy': self.strategy_name,
            'ref_id': 'test',
            'action': RequestActions.QUERY_ORDERS.value,
            'metadata': metadata
        }
        self.requests.append(
            [IntercomScope.TRADE.value + ':eaas_execution' + '_request', json.dumps(request), PublishChannel.PDT.value])

    def on_response(self, response):
        response = json.loads(response['data'])
        print('response: ', response)
        print('balance: ', response['action'], RequestActions.QUERY_BALANCE.value)
        if response['action'] == RequestActions.QUERY_BALANCE.value:
            balance = response['metadata']['metadata']
            print('balance: ', balance)
            balance_list = []
            for _ in balance:
                if isinstance(balance[_], dict) and 'total' in balance[_] and balance[_]['total'] > 0:
                    balance_list.append([_, balance[_]['total'], balance[_]['available'], balance[_]['reserved']])
            balance_pd = pd.DataFrame(balance_list, columns=['symbol', 'total', 'available', 'reserved'])
            print('balance: ', balance_pd)
            balance_pd.to_csv('{}_{}_balance_info.csv'.format(self.exchange, self.account))
        elif response['action'] == RequestActions.QUERY_ORDERS.value:
            if not response['metadata']['metadata']['result']:
                print('response: ', response)
                return

            orders = response['metadata']['metadata']['orders']

            orders = pd.DataFrame(orders)
            file_name = f"{response['metadata']['exchange']}_{response['metadata']['symbol']}_{response['metadata']['metadata']['account_id']}"
            orders.to_csv(file_name)

    def cancel_order(self, order_id, contract_type='spot', direction=Direction.SELL.value, strategy_key='manual'):
        request = {
            'strategy': self.strategy_name,
            'ref_id': f'{self.exchange}_{self.symbol}_{time.strftime("%Y%m%d%H%M%S", time.localtime())}',
            'action': OrderActions.CANCEL.value,
            'metadata': {
                'exchange': self.exchange,
                'symbol': self.symbol,
                'order_id': order_id,
                'contract_type': contract_type,
                'account_id': self.account,
                'direction': direction,
                'strategy_key': strategy_key
            }
        }
        logger.info(f"CancelOrder => {request['ref_id']}")
        self.send_request([self.trade_request_key, json.dumps(request), PublishChannel.PDT.value])

    def on_orderbook_ready(self, orderbook):
        # print('orderbook： ', orderbook)
        key = '|'.join([orderbook['exchange'], orderbook['symbol'], orderbook['contract_type'], 'orderbook'])
        self.market_data[key] = orderbook
        print(orderbook['exchange'], self.exchange, orderbook['symbol'], self.symbol)
        if orderbook['exchange'] == self.exchange and orderbook['symbol'] == self.symbol:
            if self.orderbook_cocunter != 0:
                return
            self.orderbook_cocunter += 1
            asks = orderbook['metadata']['asks']
            bids = orderbook['metadata']['bids']
            snapshot = []
            if len(asks) > len(bids):
                for i in range(len(asks) - len(bids)):
                    bids.append([None, None])
            elif len(bids) > len(asks):
                for i in range(len(bids) - len(asks)):
                    asks.append([None, None])
            for ask, bid in zip(asks, bids):
                snapshot.append([orderbook['exchange'], orderbook['symbol'], ask[0], ask[1], bid[0], bid[1]])
            if len(snapshot) > 0:
                s_pd = pd.DataFrame(snapshot,
                                    columns=['exchange', 'symbol', 'ask_px', 'ask_size', 'bid_px', 'bid_size'])
                s_pd.to_csv('{}_{}_snapshot'.format(self.exchange, self.symbol))
            # print('snapshot:',snapshot)

    def on_kline_ready(self, kline):

        timestamp = kline['metadata']['timestamp']
        if self.last_kline_timestamp != timestamp:
            self.market_cum_vol += self.last_kline_vol
            self.last_kline_timestamp = timestamp
            timeArray = time.localtime(timestamp / 1000)
            otherStyleTime = time.strftime("%Y--%m--%d %H:%M:%S", timeArray)
            print('kline:', kline)
            print('in format: ', otherStyleTime)
            print('cum_vol: ', self.market_cum_vol)
        else:
            self.last_kline_vol = kline['metadata']['valume']

    def send_order(self, price=None, quantity=None, direction=None, contract_type='spot',
                   order_type=OrderType.LIMIT.value, strategy_key='manual', delay=None, post_only=False):
        order = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "contract_type": contract_type,
            "account_type": 'exchange',
            "price": price,
            "quantity": quantity,
            "direction": direction,
            "order_type": order_type,
            "account_id": self.account,
            "strategy_key": strategy_key,
            "delay": delay if delay is not None else 59000,
            "post_only": post_only,
            # notes 字段会以json写入数据库
            "notes": {
                "task_id": 'manual'
            }
        }

        request = {
            'strategy': self.strategy_name,
            'ref_id': f'{order["exchange"]}_{order["symbol"]}_{time.strftime("%Y%m%d%H%M%S", time.localtime())}',
            'action': OrderActions.SEND.value,
            'metadata': order
        }
        order_info = f"{order['account_id']} {order['strategy_key']} {order['exchange']} {order['symbol']} {order['order_type']} {order['direction']} {order['quantity']}@{order['price']}"
        logger.info(f"SendOrder => {request['ref_id']} {order_info} {request['strategy']}")
        self.send_request([self.trade_request_key, json.dumps(request), PublishChannel.PDT.value])
        # self.requests.append([[self.trade_request_key, json.dumps(request, PublishChannel.PDT.value)])

    def get_orderbook_snapshot(self, exchange, symbol):
        key = '|'.join(exchange, symbol, 'spot', 'orderbook')
        if key in self.market_data:
            data = self.market_data[key]
            print(data)

    def send_request(self, req, rtype='PDT', channel=PublishChannel.PDT.value):
        self.requests.append({
            'rtype': rtype,
            'channel': channel,
            'request': req
        })

    def on_book(self, market_data):
        """
        listen market data from pdt
        :param market_data:
        :return:
        """
        try:
            market_data = json.loads(market_data['data'])
            # 当前行情的时效性不够
            # if not orderbook_validate(market_data, 3):
            #     return
            if market_data['data_type'] == MarketDataType.QUOTE.value:
                self.on_quote_ready(market_data)
            elif market_data['data_type'] == MarketDataType.ORDERBOOK.value:
                self.on_orderbook_ready(market_data)
            elif market_data['data_type'] == MarketDataType.TRADE.value:
                self.on_trade_ready(market_data)
            elif market_data['data_type'] == MarketDataType.FUNDING.value:
                self.on_funding_ready(market_data)
            elif market_data['data_type'] == MarketDataType.INDEX.value:
                self.on_index_ready(market_data)
            elif market_data['data_type'] == MarketDataType.KLINE.value:
                self.on_kline_ready(market_data)
            elif market_data['data_type'] == MarketDataType.QUOTETICKER.value:
                self.on_quote_ticker_ready(market_data)
            else:
                logger.error(market_data)
                logger.error(f'wrong type of market data: {market_data["data_type"]}')
        except Exception as e:
            logger.error(e)
            # sentry.captureException()


if __name__ == '__main__':
    toolkit_instance = Toolkit('Bgogo', 'BTCUSD', 'nam')
    # toolkit_instance.cancel_order('52807934098')
    # toolkit_instance.send_order(0.001,1, Direction.BUY.value)
    # toolkit_instance.send_order(0.00000457,10692.19, Direction.SELL.value)
    toolkit_instance.get_balance()
    # toolkit_instance.get_active_order()
    toolkit_instance.run()
