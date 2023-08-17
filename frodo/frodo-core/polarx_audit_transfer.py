#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 建议 pip install ujson tqdm -i https://mirrors.aliyun.com/pypi/simple 安装这两个包, 展示效果和解析性能比较好
# 想要看解析进度需要安装tqdm
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
import operator

"""
-- 解析OMA的JSON文件，生成cloudbench需要的格式, 输入JSON格式
{"__source__":"11.115.109.200","__time__":"1659526965","__topic__":"polardbx_sqlaudit","affect_rows":"0","autocommit":"1","ccl_hit_cache":"","ccl_status":"","ccl_wait_time":"","client_ip":"100.121.6.153","client_port":"42490","db_name":"information_schema","fail":"0","fetched_rows":"","instance_id":"pxc-hzrcjtfe9db8y6","is_prepare_stmt":"0","matched_ccl_rule":"","parameters":"","prepare_stmt_id":"0","response_time":"0.029","sql":"select 1","sql_code":"","sql_time":"2022-08-03 19:42:45.307","sql_type":"Select","trace_id":"14b351c419001000","transaction_id":"14b351c419001000","transaction_policy":"","user":"zkk_test","workload_type":"TP"}
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
#        for line in f.readlines():
        for line in f:
            try:
                line = str(line).strip()
                if line:
                    n += 1
                    data_dict = json.loads(line)
                    if data_dict:
                       dst={}
                       dst["convertSqlText"]=data_dict["sql"]
                       dst["parameter"]=data_dict["parameters"]
                       if dst["convertSqlText"].endswith(" more") or (operator.contains(dst["convertSqlText"],'?') and (dst["parameter"]==None or dst["parameter"]=="" )):
                           print("ignore truncated sql")
                           continue
                       dst["startTime"]=parse_time(data_dict["sql_time"])
                       dst["session"]=data_dict["client_ip"]+":"+data_dict["client_port"]
                       dst["execTime"]=int(string.atof(data_dict["response_time"])*1000)
                       dst["schema"]=data_dict["db_name"]
                       #dst["xid"]=data_dict["transaction_id"]
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