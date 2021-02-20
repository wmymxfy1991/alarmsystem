import json
import asyncio
import os

from config.enums import *
from config.config import ROOT_PATH
from config.config import CONFIG_GLOBAL
from util.aredis import RedisHandler
from util.util import *


class OMS:
    """
    这个类是为了处理EAAS订单相关的问题, 复用REDIS_TASK_COMMAND通道
    目前的功能有:
        1. 查询已经完成的EAAS的所有订单
        2. 查询已经完成的EAAS的未处于完结状态的订单
        3. 查询已经完成的EAAS的处于完结状态的订单
    """
    def __init__(self):
        self.r_ui, self.p_ui = RedisHandler().connect(CONFIG_GLOBAL['REDIS_UI'])
        self.requests = []

    async def process_request(self, command):
        resp_data = {
            'task_id': command['task_id'],
            'type': command['type'],
            'client_id': command['client_id'],
            'status': TaskStatus.FINISHED.value,
            'result': True,
            'msg': None
        }

        file_path = os.path.join(ROOT_PATH, 'orders', f'{command["task_id"]}.json')
        with open(file_path, 'r') as f:
            order_data = json.load(f)
            if command['type'] == Command.OMS_ORDER_STATUS.value:
                resp_data['msg'] = order_data
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))
            elif command['type'] == Command.OMS_UNFINISHED_ORDERS.value:
                resp_data['msg'] = {
                    'pending_orders': order_data['pending_orders'],
                    'active_orders': order_data['active_orders'],
                }
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))
            elif command['type'] == Command.OMS_FINISHED_ORDERS.value:
                resp_data['msg'] = {
                    'link': '',
                    'finished_orders': order_data['finished_orders'],
                }
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))
            elif command['type'] == Command.STATISTICS.value:
                strat_info = {}
                flag = False
                for strategy_id in command['strategies']:
                    if not order_data['finished_orders'][strategy_id] and not order_data['active_orders'][strategy_id]:
                        # finished_orders is None, there is no deal
                        strat_info[strategy_id] = {}
                        continue
                    flag = True
                    strat_info[strategy_id] = cal_orders(
                        dict(order_data['finished_orders'][strategy_id], **order_data['active_orders'][strategy_id]),
                        command['start_time'], command['end_time'],
                        command['strategies'][strategy_id]['exchange_fee'],
                        command['strategies'][strategy_id]['service_fee'],
                        command['strategies'][strategy_id]['currency_type']
                    )
                resp_data['msg'] = strat_info
                resp_data['result'] = flag
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))

            elif command['type'] == Command.EXPORT_STATISTICS.value:
                base = command['symbol'][1]
                quote = command['symbol'][2]
                ors_stat_info = cal_orders(dict(order_data['finished_orders'], **order_data['active_orders']),
                                           command['start_time'], command['end_time'], command['exchange_fee'],
                                           0 if 'service_fee' not in command or command['service_fee'] is None else
                                           command['service_fee'], command['currency_type'])

                link = create_export_statistics(command, ors_stat_info, '-', '-', base, quote, False,
                                                ors_stat_info['coin_cost'])
                resp_data = {
                    'task_id': command['task_id'],
                    'client_id': command['client_id'],
                    "type": Command.EXPORT_STATISTICS.value,
                    'msg': link
                }
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))

            elif command['type'] == Command.DOWNLOAD.value:
                all_links, _ = create_execution_report(command, order_data)
                
                resp_data = {
                    'task_id': command['task_id'],
                    'client_id': command['client_id'],
                    "type": Command.DOWNLOAD.value,
                    'msg': all_links
                }
                await self.r_ui.publish(CONFIG_GLOBAL['REDIS_TASK_COMMAND_RESP'], json.dumps(resp_data))

    def command_handler(self, command):
        """
        接收来自PDTUI的命令, 推送至队列等待处理
        """
        command = json.loads(command['data'])
        if command['type'] not in [Command.OMS_ORDER_STATUS.value, Command.OMS_UNFINISHED_ORDERS.value,
                                   Command.OMS_FINISHED_ORDERS.value, Command.STATISTICS.value, Command.DOWNLOAD.value,
                                   Command.EXPORT_STATISTICS.value]:
            return

        print(f"OMS get command => {json.dumps(command)}")
        file_path = os.path.join(ROOT_PATH, 'orders', f'{command["task_id"]}.json')
        if os.path.isfile(file_path):  # 只有当Task处于完结状态的时候才使用OMS查询
            print(f"找到本地文件, 开始处理 {file_path}")
            self.requests.append(command)

    async def main_process(self):
        await self.p_ui.subscribe(**{CONFIG_GLOBAL['REDIS_TASK_COMMAND']: self.command_handler})
        while True:
            if len(self.requests) > 0:
                for req in self.requests:
                    await self.process_request(req)
                self.requests = []

            await self.p_ui.get_message(timeout=0.1)
            await asyncio.sleep(0.001)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_process())


if __name__ == '__main__':
    oms_instance = OMS()
    oms_instance.run()
