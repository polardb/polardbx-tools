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
import re

"""
-- 解析OMA的JSON文件，生成cloudbench需要的格式, 输入JSON格式
[2023-02-10 00:03:15,398] INFO [pool-38-thread-255] c.a.c.a.f.l.AccessLog.info - Client=100.81.136.152 Total_time=0 Exec_time=0 Queue_time=0 - [2023-02-10 00:03:15 398] 1000 SYNC TABLE_ACCESS_TRAFFIC\;process=2023021000031510008113609309999067304\;CLUSTER=dailybuild
"""

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

DELIMITER = ","
start_pattern=r'^\[20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\] .*'
def parse_json_to_frodo_file(file_name, out_file,io_buffer=10000):

    """
    单机版本
    :param file_name:
    :param out_file_prefix:
    :return:
    """
    n = 0
    print("begin analyse %s" % file_name)
    log_str=""
    fetch_start=False
    with open(file_name, "r") as f,open(out_file, "a") as tof:
#         for line in tqdm.tqdm(f):
#        for line in f.readlines():
        for line in f:
            try:
                line = str(line).strip()
                if fetch_start:
                    if re.match(start_pattern,line):
                        fetch_start=True
                        # todo 解析日志行，解析出sql日期等信息
                        dst={}
                        x=re.split(r'\s+',log_str)
                        date_str=x[0][1:]+" "+x[1][:-1]
                        client_str=x[6]+x[3]
                        dst["startTime"]=parse_time(date_str)
                        dst["user"]=''
                        dst["session"]=client_str
                        dst["schema"]='cgljfl'
                        sql_text_arr=re.split(r'\[20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \d{3}\] \d* |\\;',log_str)
                        sql=sql_text_arr[1].strip()
                        rt_str=x[7]
                        rt_str_arr=re.split(r',|=',rt_str)
                        dst["execTime"]=int(rt_str_arr[1])*1000
                        if not sql.endswith(" more") and not sql.startswith('SYNC'):
                            n=n+1
                            dst["convertSqlText"]=sql
                            out_line = json.dumps(dst,sort_keys=True) + "\n"
                            tof.write(out_line)
                            if n % io_buffer == 0:
                                tof.flush()
                        else:
                            print("ignore truncated sql")
                        log_str=line
                    else:
                        log_str+="\n"+line
                else:
                    if re.match(start_pattern,line):
                        fetch_start=True
                        log_str=line
            except(Exception):
                print(line)
                print(traceback.format_exc())
                sys.exit(1)
    print("--- end ---")


def parse_time(str):
    dt=datetime.strptime(str,"%Y-%m-%d %H:%M:%S,%f")
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