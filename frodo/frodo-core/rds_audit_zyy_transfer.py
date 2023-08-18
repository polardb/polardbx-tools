#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 建议 pip install ujson tqdm -i https://mirrors.aliyun.com/pypi/simple 安装这两个包, 展示效果和解析性能比较好
# 想要看解析进度需要安装tqdm
#用于解析rds 专有云审计日志
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
import codecs
import csv
"""
-- 解析专有云rds csv审计日志
client ip,db name,user,SQL command,thread id,cost time(microsecond),number of return,execute time,
189.33.68.214,rds_mpdwe_uat1,mpdwe_dpl,"select
    10 * 60 - TIMESTAMPDIFF(SECOND,update_time,sysdate())
    from comm_monitor_log_info
    limit 1",2768610,0,1,Fri Feb 03 16:50:30 CST 2023,

"""

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

DELIMITER = ","


def parse_rds_audit_zyy_to_frodo_file(file_name, out_file,io_buffer=10000):

    """
    单机版本
    :param file_name:
    :param out_file_prefix:
    :return:
    """
    n = 0
    print("begin analyse %s" % file_name)
    with codecs.open(file_name, encoding='utf-8') as f,open(out_file, "a") as tof:
        for row in csv.DictReader(f, skipinitialspace=True):
            try:
                if row:
                    dst={}
                    dst["convertSqlText"]=row["SQL command"]
                    if dst["convertSqlText"].startswith('log'):
                        continue
                    startTime=row["execute time"]
                    dst["startTime"]=parse_time(startTime)
                    dst["session"]=row["thread id"]
                    dst["execTime"]=int(row["cost time(microsecond)"])
                    dst["schema"]=row["db name"]
                    dst["user"]=row["user"]
                    out_line = json.dumps(dst) + "\n"
                    tof.write(out_line)
                if n % io_buffer == 0:
                    tof.flush()
            except(Exception):
                print(row)
                print(traceback.format_exc())
                sys.exit(1)
    print("--- end ---")


def parse_time(str):
    dt=datetime.strptime(str,"%a %b %d %H:%M:%S CST %Y")
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
        parse_rds_audit_zyy_to_frodo_file(opts.src, opts.dest)
    else:
        print("ERROR: need -s and -d")