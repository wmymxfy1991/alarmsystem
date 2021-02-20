import sys
import psutil
from prettytable import PrettyTable
from config.config import CONFIG_GLOBAL
from subprocess import call

handler_order = {
    CONFIG_GLOBAL['MASTER_HANDLER']: 1,
    CONFIG_GLOBAL['BALANCE_HANDLER']: 2,
    CONFIG_GLOBAL['ORDER_HANDLER']: 3,
    CONFIG_GLOBAL['TASK_HANDLER']: 4
}


def ps_filter():
    eaas = []
    for proc in psutil.process_iter(attrs=['pid', 'open_files', 'cmdline', 'connections']):
        cmdline = proc.info['cmdline']
        if len(cmdline) == 3 and cmdline[0] == 'python' and cmdline[2] == '2.0':
            if CONFIG_GLOBAL['TASK_HANDLER'] in cmdline[1] or CONFIG_GLOBAL['BALANCE_HANDLER'] in cmdline[1] or \
                    CONFIG_GLOBAL['ORDER_HANDLER'] in cmdline[1] or CONFIG_GLOBAL['MASTER_HANDLER'] in cmdline[1]:
                if cmdline[1][0:2] == './':
                    cmdline[1] = cmdline[1][2:]
                eaas.append(proc.info)
    return sorted(eaas, key=lambda d: handler_order[d['cmdline'][1]])


def start():
    master_flag = False
    for proc in ps_filter():
        if CONFIG_GLOBAL['MASTER_HANDLER'] in proc['cmdline'][1]:
            master_flag = True
            break
    if not master_flag:
        call(f"python ./{CONFIG_GLOBAL['MASTER_HANDLER']} 2.0 &", shell=True)


def restart():
    soft_kill()
    start()


def terminate():
    for proc in ps_filter():
        psutil.Process(proc['pid']).terminate()


def soft_kill():
    for proc in ps_filter():
        if CONFIG_GLOBAL['TASK_HANDLER'] not in proc['cmdline'][1]:
            psutil.Process(proc['pid']).terminate()
    stop_sleep()


def stop_sleep():
    for proc in ps_filter():
        if len(proc['connections']) == 1:
            # 只建立一个连接, 说明程序处于等待redis传来task的阶段, 可以结束
            psutil.Process(proc['pid']).terminate()


def show_all():
    driver_count = 0
    running_count = 0
    table = PrettyTable(["PID", "NAME", "FILE"])
    table.align["FILE"] = "l"
    for proc in ps_filter():
        if CONFIG_GLOBAL['TASK_HANDLER'] in proc['cmdline'][1]:
            driver_count += 1
        log_file = ''
        if len(proc['open_files']) == 1:
            running_count += 1
            log_file = proc['open_files'][0][0]
        table.add_row([proc['pid'], proc['cmdline'][1] + ' ' + proc['cmdline'][2], log_file])
    print(f'\n一共有{driver_count}个Driver实例, {running_count}个正在执行Task')
    print(table)


def clean():
    pass


functions = {
    'start': start,
    'restart': restart,
    'terminate': terminate,
    'soft_kill': soft_kill,
    'stop_sleep': stop_sleep,
    'check': show_all,
    'clean': clean
}

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] in functions:
        functions[sys.argv[1]]()
        sys.exit(0)

    print('''请选择以下功能: 
start:      开启master程序, 启动driver
restart:    重启master程序, 关闭空闲driver, 重启拉起, 让新代码生效
terminate:  暴力杀掉所有eaas进程 (请谨慎!!!!!!!!)
soft_kill:  关闭除了正在执行任务driver进程之外的eaas进程
stop_sleep: 关闭空闲driver, 并重启 (适用于仅更新driver, 算法层面的改动)
check:      查看eaas所有进程的状态
clean:      清理日志文件(请慎用, 暂不实现)
''')
