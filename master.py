import os
import json
import time
import asyncio
from config.config import *
from config.enums import *
from util.aredis import RedisHandler
from subprocess import call, check_output


class Master:
    def __init__(self):
        self.process_num = 4 * os.cpu_count()
        self.config = CONFIG_GLOBAL
        self.r_alarm, self.p_alarm = RedisHandler().connect(CONFIG_GLOBAL['REDIS_ALARM'])

    async def main_process(self):
        print(f'Start EAAS2.0 master process with core number {self.process_num} \n')

        cmd_common = "ps jxf | awk '$1==1{print $0}' | grep"
        cmd_driver_check = f"{cmd_common} '{CONFIG_GLOBAL['TASK_HANDLER']} 2.0' | wc -l"
        cmd_balance_check = f"{cmd_common} '{CONFIG_GLOBAL['BALANCE_HANDLER']} 2.0' | wc -l"
        cmd_order_check = f"{cmd_common} '{CONFIG_GLOBAL['ORDER_HANDLER']} 2.0' | wc -l"
        heartbeat_count = 60
        while True:
            # 保证Driver的数量
            driver_num = int(check_output(cmd_driver_check, shell=True))
            if driver_num < self.process_num:
                call(f"python ./{CONFIG_GLOBAL['TASK_HANDLER']} 2.0 &", shell=True)

            balance_num = int(check_output(cmd_balance_check, shell=True))
            if balance_num < 1:
                call(f"python ./{CONFIG_GLOBAL['BALANCE_HANDLER']} 2.0 &", shell=True)

            oms_num = int(check_output(cmd_order_check, shell=True))
            if oms_num < 1:
                call(f"python ./{CONFIG_GLOBAL['ORDER_HANDLER']} 2.0 &", shell=True)

            if heartbeat_count >= 60:
                heartbeat_count = 0
                alarm_msg = {
                    "timestamp": int(time.time() * 1000),
                    "status": "normal",
                    "service": "SERVER_STATUS",
                    "server": "EAAS_PROD",
                    "type": "message"
                }
                await self.r_alarm.publish(IntercomScope.ED.value + ':' + IntercomChannel.SERVER_STATUS.value, json.dumps(alarm_msg))
            heartbeat_count += 1
            time.sleep(1)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_process())


if __name__ == '__main__':
    master_instance = Master()
    master_instance.run()
