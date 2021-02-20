# encoding: utf-8
import os
import sys
import traceback
from datetime import datetime

from config.config import ROOT_PATH
from util.alioss import alioss


def format_msg(level, *args):
    caller = traceback.extract_stack()[-3]
    filepath, filename = os.path.split(caller[0])
    msg = ' '.join([str(x) for x in args])
    return f'{datetime.now().strftime("%Y%m%d %H:%M:%S.%f")[0:-3]} {level}[{filename}/{caller[2]}:{caller[1]}] {msg}'


class EaasLog:
    def __init__(self):
        self.string_buffer = ''
        self.file_buffer = ''
        self.file_handler = None  # 打开的文件无需关闭, 因为时刻会flush, 最后会被回收机制自动关闭
        self.file_name = ''
        self.file_link = ''
        self.oss_pos = 0
        self.local_debug = False

    def init(self, file_name, local_debug=False):
        self.local_debug = local_debug
        if local_debug:
            return

        self.file_name = file_name
        self.file_link = alioss.sign_url(file_name)
        self.oss_pos = alioss.update_pos_if_exist(file_name)

        log_dir = os.path.join(ROOT_PATH, 'log')
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        self.file_handler = open(os.path.join(log_dir, file_name), 'a+')

    def flush(self):
        try:
            if len(self.string_buffer) > 0:
                # 向阿里云oss写入日志信息, 并后移oss文件指针
                self.oss_pos = alioss.file_append(self.file_name, self.oss_pos, self.string_buffer)
                self.string_buffer = ''

            if len(self.file_buffer) > 0:
                # 写入本地日志文件, 并且flush
                self.file_handler.write(self.file_buffer)
                self.file_buffer = ''
                self.file_handler.flush()
        except Exception:  # 如果发生异常, 就下次再flush
            pass

    def debug(self, *args):
        if self.local_debug:
            print(format_msg('DEBUG ', *args))
        else:
            self.string_buffer += format_msg('DEBUG ', *args) + '\n'
            self.file_buffer += format_msg('', *args) + '\n'

    def info(self, *args):
        if self.local_debug:
            print(format_msg('INFO ', *args))
        else:
            self.string_buffer += format_msg('INFO ', *args) + '\n'

    def warning(self, *args):
        if self.local_debug:
            print(format_msg('WARNING ', *args))
        else:
            self.string_buffer += format_msg('WARNING ', *args) + '\n'

    def error(self, *args):
        if self.local_debug:
            print(format_msg('ERROR ', *args))
        else:
            self.string_buffer += format_msg('ERROR ', *args) + '\n'

    # 写本地日志文件
    def file(self, *args):
        if self.local_debug:
            print(format_msg('FILE ', *args))
        else:
            self.file_buffer += format_msg('', *args) + '\n'


logger = EaasLog()
