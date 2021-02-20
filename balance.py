import json
import asyncio

from config.enums import *
from config.config import *
from util.aredis import RedisHandler


class Balance:
    """
    这个类是为了处理一些对所有EAAS都有效的问题而提供的公用辅助函数
    目前的功能有:
        1. 查询对应交易所对应账户的余额
        2. 下载特定Task在某个时间段的统计信息
        3. 查询特定的Task最新的运行状态, 当PDTUI因为特定原因丢失Task状态的时候使用
    """

    def __init__(self):
        self.r_ui, self.p_ui = RedisHandler().connect(CONFIG_GLOBAL['REDIS_UI'])
        self.r_pdt, self.p_pdt = RedisHandler().connect(CONFIG_GLOBAL['REDIS_PDT'])
        self.requests = []
        self.strategy_name = CONFIG_GLOBAL['STRATEGY_NAME']

    async def process_request(self, req):
        keyword, body, rtype = req
        if rtype == PublishChannel.PDT.value:
            await self.r_pdt.publish(keyword, body)
        elif rtype == PublishChannel.UI.value:
            await self.r_ui.publish(keyword, body)
        elif rtype == MasterCommand.INSPECT.value:
            status = await self.r_ui.hget(CONFIG_GLOBAL['REDIS_STATUS_MONITOR'], body['task_id'])
            if status is None:
                status = await self.r_ui.hget('test_' + CONFIG_GLOBAL['REDIS_STATUS_MONITOR'], body['task_id'])
            if status is not None:
                status = json.loads(status)
                status['client_id'] = body['client_id']
                status['result'] = True
            else:
                status = {'client_id': body['client_id'], 'result': False}
            status = json.dumps(status)
            await self.r_ui.publish(keyword, status)

    def command_handler(self, command):
        """
        接收来自PDTUI的命令, blpop eaas_master_command队列, 处理完把结果publish至redis
        :param command:  目前有三种命令:
        1. 查询余额 {"type":"get_balance","exchange":"Hotbit","client_id":1570863741413,"account":"tuber","test_mode":false}
        2. 统计信息下载 {"type":"download","client_id":1570873969068,"start_time":"2019-10-02 03:30:00","end_time":"2019-10-12 17:46:33","task":{"algorithm":"TWAP","exchange":"Bittrex","account":"trading","symbol":["WAXPBTC","WAXP","BTC"],"direction":"Sell","currency_type":"Base","total_size":4100100,"trade_role":"Taker","price_threshold":null,"exchange_fee":0.002,"execution_mode":"Passive","test_mode":false,"start_time":"2019-10-02 03:30:00","end_time":"2019-10-14 03:30:00","initial_balance":{"WAXP":4100100,"BTC":0},"task_id":"TWAP_Bittrex_WAXPBTC_20191010155258","coin_config":{"WAXPBTC":{"base_min_order_size":0.001,"quote_min_order_size":0.001,"price_precision":1e-8,"size_precision":0.001}},"customer_id":"amberai","alarm":true}}
        3. 查询Task状态 {"type":"inspect","task_id":"SAMPLE_Binance_ETHUSDT_20191003122252","client_id":1570759975824}
        :return: 函数无回报, 结果会通过redis publish到 eaas_master_command_response中
        1. 查询余额 {"client_id": 1570874319275, "action": "query_balance", "metadata": {"BTC": {"available": 0, "reserved": 0, "shortable": 0, "total": 0}, "USDT": {"available": 0, "reserved": 0, "shortable": 0, "total": 0}, "BAT": {"available": 0, "reserved": 0, "shortable": 0, "total": 0}, "account_id": "laura1", "result": true}}
        2. 统计信息下载 {"task_id": "TWAP_Bittrex_WAXPBTC_20191010155258", "client_id": 1570874267021, "type": "download", "msg": "http://eaas.oss.amberainsider.com/EAAS_TWAP_Bittrex_WAXPBTC_20191010155258.csv?OSSAccessKeyId=LTAIuRlBvUKMjDP6&Expires=4724474272&Signature=VG62Z9dETs%2B9qk9zSsqN3ZbHJLU%3D"}
        3. 查询Task状态 {"client_id": 1570759838252, "result": true, "ip": "172.31.228.79", "pid": 2669, "name": "TWAP_Bittrex_WAXPBTC_20191010155258", "exchange": "Bittrex", "account": "trading", "symbol": ["WAXPBTC", "WAXP", "BTC"], "direction": "Sell", "currency_type": "Base", "price_threshold": null, "total_size": 4100100, "deal_size": 3629797.35, "start_time": "2019-10-02 03:30:00", "end_time": "2019-10-14 03:30:00", "update_time": "2019-10-12 18:27:43.551", "status": "running", "status_msg": "\u4efb\u52a1\u6b63\u5728\u8fd0\u884c", "attention": false, "task": {"algorithm": "TWAP", "exchange": "Bittrex", "account": "trading", "symbol": ["WAXPBTC", "WAXP", "BTC"], "direction": "Sell", "currency_type": "Base", "total_size": 4100100, "trade_role": "Taker", "price_threshold": null, "exchange_fee": 0.002, "execution_mode": "Passive", "test_mode": false, "start_time": "2019-10-02 03:30:00", "end_time": "2019-10-14 03:30:00", "initial_balance": {"WAXP": 4100100, "BTC": 0}, "task_id": "TWAP_Bittrex_WAXPBTC_20191010155258", "coin_config": {"WAXPBTC": {"base_min_order_size": 0.001, "quote_min_order_size": 0.001, "price_precision": 1e-08, "size_precision": 0.001}}, "customer_id": "amberai", "alarm": true}}
        """
        print(f"Master get command => {command}")
        command = json.loads(command)
        if command['type'] == MasterCommand.GET_BALANCE.value:
            request = {
                'strategy': self.strategy_name,
                'ref_id': command['client_id'],
                'action': RequestActions.QUERY_BALANCE.value,
                'metadata': {
                    "exchange": command['exchange'],
                    'account_id': command['account'],
                    'currency': ''
                }
            }
            balance_request = f'{IntercomScope.TRADE.value}:{self.strategy_name}_request'
            if 'test_mode' in command and command['test_mode']:
                balance_request = 'Test' + balance_request
            self.requests.append([balance_request, json.dumps(request), PublishChannel.PDT.value])
        elif command['type'] == MasterCommand.INSPECT.value:
            self.requests.append([CONFIG_GLOBAL['REDIS_MASTER_COMMAND_RESP'], command, MasterCommand.INSPECT.value])

    def balance_handler(self, response):
        response = json.loads(response['data'])
        if response['action'] == RequestActions.QUERY_BALANCE.value:
            metadata = {
                "client_id": response['ref_id'],
                "action": RequestActions.QUERY_BALANCE.value,
                "metadata": response['metadata']['metadata']
            }
            # 发送查询到的balance结果给PDTUI
            self.requests.append([CONFIG_GLOBAL['REDIS_MASTER_COMMAND_RESP'], json.dumps(metadata), PublishChannel.UI.value])

    async def main_process(self):
        balance_key = f'{IntercomScope.TRADE.value}:{self.strategy_name}_response'
        await self.p_pdt.subscribe(**{
            balance_key: self.balance_handler,
            'Test' + balance_key: self.balance_handler
        })

        while True:
            if len(self.requests) > 0:
                for req in self.requests:
                    await self.process_request(req)
                self.requests = []

            await self.p_pdt.get_message(timeout=0.1)
            await asyncio.sleep(0.001)

            command = await self.r_ui.lpop(CONFIG_GLOBAL['REDIS_MASTER_COMMAND'])
            if command is not None:
                self.command_handler(command)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_process())


if __name__ == '__main__':
    balance_instance = Balance()
    balance_instance.run()
