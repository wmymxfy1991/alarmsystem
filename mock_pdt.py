# encoding: utf-8
# 主要模拟PDT在交易模块的API

import asyncio
import json
import uuid
import time
import sys
import traceback
import random
import copy
from config.enums import *
from config.config import CONFIG_GLOBAL
from util.aredis import RedisHandler


class PDT:
    def __init__(self):
        self.r, self.p = RedisHandler().connect(CONFIG_GLOBAL['REDIS_PDT'])
        self.Balance = {}
        self.Orders = {}
        self.Contracts = {
            "PAXUSDT": ["PAX", "USDT"],
            "USDCUSDT": ["USDC", "USDT"],
            "TUSDUSDT": ["TUSD", "USDT"],
            "FETUSDT": ["FET", "USDT"],
            "MFTBTC": ["MFT", "BTC"],
            "QKCBTC": ["QKC", "BTC"],
            "OMGBTC": ["OMG", "BTC"],
            "KNCBTC": ["KNC", "BTC"],
            "BNBUSDT": ["BNB", "USDT"],
            "IOTXBTC": ["IOTX", "BTC"],
            "ICXBTC": ["ICX", "BTC"],
            "PERLBTC": ["PERL", "BTC"],
            "PERLUSDT": ["PERL", "USDT"],
            "PERLBNB": ["PERL", "BNB"],
            "PERLUSDC": ["PERL", "USDC"],
            "HTUSDT": ["HT", "USDT"],
            "ALGOUSDT": ["ALGO", "USDT"],
            "TTUSDT": ["TT", "USDT"],
            "ALGOBTC": ["ALGO", "BTC"],
            "TTBTC": ["TT", "BTC"],
            "ETHBTC": ["ETH", "BTC"],
            "CREBTC": ["CRE", "BTC"],
            "CREUSDT": ["CRE", "USDT"],
            "BSVBTC": ["BSV", "BTC"],
            "DXUSDT": ["DX", "USDT"],
            "VIDYUSDT": ["VIDY", "USDT"],
            "BKCUSDT": ["BKC", "USDT"],
            "ARPAUSDT": ["ARPA", "USDT"],
            "ECOMUSDT": ["ECOM", "USDT"],
            "ECOMETH": ["ECOM", "ETH"],
            "GRINUSDT": ["GRIN", "USDT"],
            "BEAMBTC": ["BEAM", "BTC"],
            "BEAMUSDT": ["BEAM", "USDT"],
            "KINUSDT": ["KIN", "USDT"],
            "ETHUSDT": ["ETH", "USDT"],
            "BTCUSDT": ["BTC", "USDT"],
            "CHZBTC": ["CHZ", "BTC"],
            "LPTBTC": ["LPT", "BTC"],
            "BTCUSDC": ["BTC", "USDC"],
            "NTYUSDT": ["NTY", "USDT"],
            "PXLBTC": ["PXL", "BTC"],
            "LUNAKRW": ["LUNA", "KRW"]
        }
        self.PublishQueue = []
        self.strategy_name = CONFIG_GLOBAL['STRATEGY_NAME']

    def init_balance(self, exchange, account, symbols=[]):
        exch_balance = self.Balance.setdefault(exchange, {})
        acc_exch_balance = exch_balance.setdefault(account, {})
        balance_set = set()
        balance_set.update(symbols)
        for currency in self.Contracts:
            balance_set.update(self.Contracts[currency])
        for currency in balance_set:
            acc_exch_balance.setdefault(currency, 1000000)

    def balance_handler(self, message):
        try:
            message = message['data']
            print('balance_handler---------------', message)
            task = json.loads(message)
            self.Contracts[task['symbol'][0]] = [task['symbol'][1], task['symbol'][2]]
            symbols = [task['symbol'][1], task['symbol'][2]]
            if 'median' in task and task["median"] != "":
                self.Contracts[task['median'][0]] = [task['median'][1], task['median'][2]]
                symbols.extend([task['median'][1], task['median'][2]])
            if 'anchor' in task and task["anchor"] != "":
                self.Contracts[task['anchor'][0]] = [task['anchor'][1], task['anchor'][2]]
                symbols.extend([task['anchor'][1], task['anchor'][2]])
            self.init_balance(task['exchange'], task['account'], symbols)
        except Exception as e:
            print(e)

    async def send_balance(self):
        for exch in self.Balance:
            for acc in self.Balance[exch]:
                redis_key = f'Test{IntercomScope.POSITION.value}:{exch}|{acc}'
                acc_balance = {}
                for currency in self.Balance[exch][acc]:
                    acc_balance[currency] = {
                        "available": self.Balance[exch][acc][currency],
                        "total": self.Balance[exch][acc][currency],
                        "reserved": 0, "shortable": 0
                    }
                acc_balance["result"] = True
                acc_balance["account_id"] = acc
                self.PublishQueue.append([
                    redis_key, json.dumps({
                        "exchange": exch,
                        "account_id": acc,
                        "global_balances": {
                            "spot_balance": acc_balance
                        }
                    })
                ])

    def balance_request_handler(self, request):
        req = request['metadata']
        exch = req['exchange']
        acc = req['account_id']
        if exch not in self.Balance or acc not in self.Balance[exch]:
            self.init_balance(exch, acc)

        acc_balance = {"result": True, "account_id": acc}
        for currency in self.Balance[exch][acc]:
            acc_balance[currency] = {
                "available": self.Balance[exch][acc][currency],
                "total": self.Balance[exch][acc][currency],
                "reserved": 0, "shortable": 0
            }

        self.PublishQueue.append([
            f'Test{IntercomScope.TRADE.value}:{self.strategy_name}_response', json.dumps({
                "ref_id": request['ref_id'],
                "action": RequestActions.QUERY_BALANCE.value,
                "metadata": {"metadata": acc_balance}
            })
        ])

    def match_engine(self, action, order):
        cur_time = time.strftime("%Y%m%d%H%M%S000", time.localtime())
        body = {
            "exchange": order["exchange"],
            "symbol": order["symbol"],
            "contract_type": order["contract_type"],
            "timestamp": cur_time
        }
        if action == OrderActions.INSPECT.value:
            origin_order = self.Orders[order["order_id"]]
            body.update({
                "event": RequestActions.INSPECT_ORDER.value,
                "metadata": {
                    "result": True,
                    "account_id": order["account_id"],
                    "order_id": order["order_id"]
                },
                "order_info": {
                    "original_amount": origin_order["quantity"],
                    "filled": origin_order["filled"],
                    "status": origin_order["status"],
                    "avg_executed_price": origin_order["price"]
                },
            })

            # 如果订单没有成交完, 继续成交
            if origin_order["filled"] < origin_order["quantity"] and origin_order["status"] != OrderStatus.CANCELLED.value:
                self.random_filled(origin_order)

        if action == OrderActions.CANCEL.value:
            origin_order = self.Orders[order["order_id"]]
            origin_order["status"] = OrderStatus.CANCELLED.value
            body.update({
                "event": RequestActions.CANCEL_ORDER.value,
                "metadata": {
                    "result": True,
                    "account_id": order["account_id"],
                    "order_id": order["order_id"]
                }
            })
        if action == OrderActions.SEND.value:
            order_id = str(uuid.uuid1())
            self.Orders[order_id] = order
            order["filled"] = 0
            body.update({
                "event": RequestActions.SEND_ORDER.value,
                "metadata": {
                    "result": True,
                    "account_id": order["account_id"],
                }
            })
            if self.check_enough_balance(order):
                body["metadata"]["order_id"] = order_id
                filled = self.random_filled(order)
                if order['order_type'] == 'fak':
                    body.update({
                        "event": RequestActions.INSPECT_ORDER.value,
                        "metadata": {
                            "result": True,
                            "account_id": order["account_id"],
                            "order_id": order_id,
                            "inspect_result": True
                        },
                        "order_info": {
                            "original_amount": order["quantity"],
                            "filled": order["filled"],
                            "status": OrderStatus.FILLED.value,
                            "avg_executed_price": order["price"]
                        }
                    })
                    if filled != order["quantity"]:
                        order["status"] = OrderStatus.CANCELLED.value
                        body["metadata"]["cancel_result"] = True
                        body["order_info"]["status"] = OrderStatus.CANCELLED.value
            else:
                order["status"] = OrderStatus.REJECTED.value
                body["metadata"].update({
                    "result": False,
                    "error_code": "999999",
                    "error_code_msg": "Available balance is not enough"
                })
        return body

    def random_filled(self, order):
        aleady_filled = order["filled"]
        filled = random.uniform(0, order["quantity"]) * 10000 // 1 / 10000  # 四位小数
        filled = order["quantity"] - aleady_filled if filled + aleady_filled > 0.5 * order["quantity"] else filled
        order["filled"] = filled + aleady_filled
        order["status"] = OrderStatus.FILLED.value if order["filled"] == order["quantity"] else OrderStatus.PARTIALLY_FILLED.value

        balance = self.Balance[order["exchange"]][order["account_id"]]
        base, quote = self.Contracts[order["symbol"]]
        if order['direction'] == Direction.BUY.value:
            balance[base] += filled
            balance[quote] -= filled * order["price"]
        else:
            balance[base] -= filled
            balance[quote] += filled * order["price"]
        return order["filled"]

    def check_enough_balance(self, order):
        if order["exchange"] not in self.Balance or order["account_id"] not in self.Balance[order["exchange"]]:
            self.init_balance(order["exchange"], order["account_id"])
        balance = self.Balance[order["exchange"]][order["account_id"]]
        base, quote = self.Contracts[order["symbol"]]
        if order['direction'] == Direction.BUY.value and balance[quote] < order["quantity"] * order["price"]:
            return False
        if order['direction'] == Direction.SELL.value and balance[base] < order["quantity"]:
            return False
        return True

    def trade_handler(self, request):
        try:
            request = json.loads(request['data'])
            print('trade_handler------------', request)
            if request['action'] == RequestActions.QUERY_BALANCE.value:
                return self.balance_request_handler(request)

            order = copy.deepcopy(request['metadata'])
            print('order_info------------', order)
            body = self.match_engine(request['action'], order)
            body['request'] = request
            self.PublishQueue.append([
                f'Test{IntercomScope.TRADE.value}:{self.strategy_name}_response', json.dumps({
                    "ref_id": request['ref_id'],
                    "action": request['action'],
                    "strategy": self.strategy_name,
                    "metadata": body
                })
            ])
        except Exception as e:
            print(e)
            traceback.print_exc(file=sys.stdout)

    async def main_process(self):
        # 异步监听redis频道, 往对应的策略handler中推送
        await self.p.subscribe(**{
            f'Test{IntercomScope.POSITION.value}:Poll Position Request': self.balance_handler,
            f'Test{IntercomScope.TRADE.value}:{self.strategy_name}_request': self.trade_handler
        })

        send_balance_timer = 0
        while True:
            await self.p.get_message(timeout=1)
            send_balance_timer += 1
            if send_balance_timer % 3 == 0:
                await self.send_balance()

            if len(self.PublishQueue) > 0:
                for queue in self.PublishQueue:
                    await self.r.publish(queue[0], queue[1])
                self.PublishQueue = []
            await asyncio.sleep(0.001)


if __name__ == '__main__':
    pdt = PDT()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pdt.main_process())
