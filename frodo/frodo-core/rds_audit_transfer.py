#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 建议 pip install ujson tqdm -i https://mirrors.aliyun.com/pypi/simple 安装这两个包, 展示效果和解析性能比较好
# 想要看解析进度需要安装tqdm
#用于解析rds 审计日志
# import tqdm
# import ujson as json

import json
import traceback
import sys
import math
import random
import time
from datetime import datetime
import string
import os

"""
-- 解析OMA的JSON文件，生成cloudbench需要的格式, 输入JSON格式
{"__source__":"log_service","__tag__:__receive_time__":"1659953800","__time__":"1659953787","__topic__":"rds_audit_log","check_rows":"1","client_ip":"100.104.205.126","db":"zkk_test","fail":"0","instance_id":"rm-hp3n987f2o5c87rbk","latency":"97","origin_time":"1659953787658842","return_rows":"1","sql":"select 1","thread_id":"302","update_rows":"0","user":"zkk_test"}
"""

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

DELIMITER = ","


def parse_json_to_frodo_file(file_name, out_file,io_buffer=10000):

    """
    单机版本
    :param file_name:
    :param out_file_prefix:
    :return:
    """
    n = 0
    print("begin analyse %s" % file_name)
    with open(file_name, "r") as f,open(out_file, "a") as tof:
#         for line in tqdm.tqdm(f):
        for line in f:
            try:
                line = str(line).strip()
                if line:
                    n += 1
                    data_dict = json.loads(line)
                    if data_dict:
                       dst={}
                       dst["convertSqlText"]=data_dict["sql"]
                       if dst["convertSqlText"].startswith('log'):
                           continue
                       dst["startTime"]=int(data_dict["origin_time"])
                       dst["session"]=data_dict["thread_id"]
                       dst["execTime"]=int(data_dict["latency"])
                       dst["schema"]=data_dict["db"]
                       dst["user"]=data_dict["user"]
                       out_line = json.dumps(dst) + "\n"
                       tof.write(out_line)
                    if n % io_buffer == 0:
                       tof.flush()
            except(Exception):
                print(line)
                print(traceback.format_exc())
                sys.exit(1)
    print("--- end ---")


def parse_time(str):
    dt=datetime.strptime(str,"%Y-%m-%d %H:%M:%S.%f")
    return int(time.mktime(dt.timetuple())*1000000+dt.microsecond)

def optparser():
    """
    解析输入参数
    :return:
    """
    from optparse import OptionParser
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage, version="%prog 1.0")
    parser.add_option("-s", "--src", action="store", dest="src", type=str, default=1, help="inuput filename")
    parser.add_option("-d", "--dest", action="store", dest="dest", type=str, help="outuput filename suffix")
    (options, args) = parser.parse_args()
    return options


if __name__ == "__main__":
    opts = optparser()
#     parse_json_to_cloudbench_file("oma_replay.json", "oma_cloudbench.txt")
    if opts.src and opts.dest:
        # 先清空文件
        with open(opts.dest, "w") as f:
            pass
        parse_json_to_frodo_file(opts.src, opts.dest)
    else:
        print("ERROR: need -s and -d")