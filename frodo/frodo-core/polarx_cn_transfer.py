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
import operator

"""
-- 解析OMA的JSON文件，生成cloudbench需要的格式, 输入JSON格式
2022-04-29 16:01:36.670 - [user=dtacenter,host=10.181.88.16,port=40952,schema=test2]  [TDDL] [TOO LONG] [V3] [len=4111] insert into test2.ord_mrt_pri_s_bill_slice_dtla_dbtb (entr_ship_id,entr_prt_site,fcst_co,sys_tm,entr_site,frgt_wgt,fcst_brch,src,wgt_levl,pick_ins_db_tm,pick_scan_emp,pick_oper_typ,snd_ins_db_tm, snd_scan_emp,snd_oper_typ,if_unload,unload_ins_db_tm,if_delv,delv_ins_db_tm,if_scan,if_scan_tm,if_sign,if_sign_tm,if_sign_irr,if_sign_irr_tm, dist_emp,dist_tm,dist_oper_typ,rec_emp,rec_tm,rec_oper_typ,cust_nm,order_id,if_arv,arv_ins_db_tm,priority,ins_typ,ord_emp,ord_start_city, ord_end_city,addr_start_org,addr_start_sup_org,addr_start_tsf,addr_end_tsf,fst_bag_id,car_id,pick_org,pick_sup_org,snd_next_cd,start_tsf, start_tsf_car_id,start_tsf_next_cd,start_tsf_out_dt,end_tsf,arv_end_tsf_dt,delv_next_cd,end_org,end_sup_org,fst_dist_dt,dist_next_org, sign_emp,rtn_flag,sign_typ,sett_wt,pick_wt,pick_vol,start_tsf_wt,start_tsf_vol,end_tsf_wt,end_tsf_vol,rtn_typ,sign_irr_typ,fcst_unload_day, fcst_sign_tm,udf_1,udf_2,udf_3,udf_4,udf_5,udf_6,udf_7,udf_8,udf_9,udf_10,udf_11,udf_12,udf_13,udf_14,udf_15,udf_16,udf_17,udf_18,udf_19,udf_20, snd_org,delv_next_sup_cd,arv_org,arv_tm,fst_dist_org,entr_site_area,entr_site_busi_prov,entr_site_city,fcst_co_area,fcst_co_busi_prov,fcst_co_city,end_org_area,end_org_busi_prov,end_org_city,dist_emp_change,sign_emp_change,prob_flag,odr_typ,prob_tm,prob_typ,prob_context,str_1,thrid_sign_site_typ,thrid_sign_site_addr,int_1,int_2,dt_1,str_4,dt_2,str_2,str_3,dt) values(432516380620717,322096,262200,null,322096,null,262200,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'2588891402555452506',null,null,null,null,null,null,null,null,null,322001,261000,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2,330002,330700,2,370001,370700,null,null,null,null,null,null,null,null,null,null,'63',null,null,null,null,null,'2003','2022-04-22 00:15:18.0','0',null,20220422) ,(462338459147397,650005,529800,null,650005,1.26,529800,null,'1-3',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'null',null,null,null,null,null,null,null,null,null,650108,528418,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,5,530000,530100,3,440001,441700,null,null,null,null,null,null,null,null,null,null,'03',null,null,null,null,null,'2003,2402','2022-04-20 07:05:45.0','0',null,20220420) ,(462333376869357,221400,570342,null,221400,2.86,570342,null,'1-3',null,null,null,null,null,null,null,null,null,null,1,'2022-04-22 01:22:12.0',null,null,null,null,'2514','2022-04-22 01:22:12.0',1,null,null,null,null,'null',1,'2022-04-22 01:22:12.0',null,1,null,null,null,null,null,223000,570472,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null, null,null,null,null,null,null,null,null,null,null,null,null,null,null,570342,570342,null,null,null,null,null,null,null,null,null,null,null,null,null,2,320001,320300,3,460000,469000,null,null,null,'XN2212950639986',null,null,null,null,null,null,'86',null,null,570342,0,'2022-04-22 01:22:12.0','2003','2022-04-17 02:15:33.0','0',null,20220417) ,(432514416344197,680745,650502,null,681866,0.02,650502,null,'0-1',null,null,null,null,null,null,null,null,null,null,1,'2022-04-22 01:22:12.0',null,null,null,null,'2024','2022-04-22 01:22:12.0',1,null,null,null,null,'2582308802913320403',1,'2022-04-22 01:22:12.0',null,1,null,null,null,null,null,510001,650108,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null, null,null,null,null,null,null,null,null,null,null,null,null,null,null,650502,650502,null,null,n... +83899 more [len=2] [] # [rt=274721,rows=106,type=000,frows=21,arows=79,scnt=3,mem=-1,mpct=-1.0,smem=-1,tmem=-1,ltc=57271881455,lotc=22820940,letc=57249060515,ptc=92126108,pstc=92126108,prstc=-1,pctc=20863,sct=0,mbt=0,ts=1651219296393,mr=0,bid=-1,pid=-1,wt=TP,em=CURSOR,lcpu=0,lmem=0,lio=0,lnet=0.0,ur=0] # 143786862cc10000, tddl version: 5.4.13-16415631
"""

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

DELIMITER = ","
start_pattern=r'^20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} - \[.*'
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
                        date_str=x[0]+" "+x[1]
                        client_str=x[3]
                        dst["startTime"]=parse_time(date_str)
                        date_str_arr=re.split(r',|=|]',client_str)
                        dst["user"]=date_str_arr[1]
                        dst["session"]=date_str_arr[3]+":"+date_str_arr[5]
                        dst["schema"]=date_str_arr[7]
                        sql_text_arr=re.split(r'\[len=\d+\]',log_str)
                        sql=sql_text_arr[1].strip()
                        sql_args_arr=re.split(r' # \[rt=',sql_text_arr[2].strip())
                        sql_args_json=sql_args_arr[0]
                        rt_str=sql_args_arr[1]
                        rt_str_arr=re.split(r',|=',rt_str)
                        dst["execTime"]=int(rt_str_arr[0])*1000
                        if operator.contains(sql,'?') and (sql_args_json=="[]"):
                            print("ignore truncated sql")
                            log_str=line
                            continue
                        if (not sql.endswith(" more")):
                            n=n+1
                            dst["convertSqlText"]=sql
                            if sql_args_arr[0]!="[]":
                                dst["parameter"]=sql_args_json
                            out_line = json.dumps(dst) + "\n"
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