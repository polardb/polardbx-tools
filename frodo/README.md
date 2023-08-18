[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://github.com/ApsaraDB/galaxysql/blob/main/LICENSE)
[![Language](https://img.shields.io/badge/Language-Java-blue.svg)](https://www.java.com/)

## Frodo是什么
#### DOA流量回放是一款异构数据库流量回放工具，工程代号Frodo，为了解决数据库交付以及运维过程中的痛点问题而生。

#### Frodo 的两个核心功能:
1. SQL兼容性评估和改造，采集真实源库sql流量，采集的时间越久，兼容性评估越完整。有助于前期准确评估改造工作量，并基于评估结果直接指导研发对不兼容SQL进行改造。
2. 基于真实SQL流量的数据库性能压测，模拟源库行为，用真实流量评估目标库的性能是否达标，彻底解决数据库平迁或者搬站场景的数据库性能压测问题。


## 编译打包
sh build.sh

## usage
### 从阿里云sls采集drds或rds日志
java -jar slssniffer-1.0.jar --endpoint=cn-hangzhou-intranet.log.aliyuncs.com --project=xxx  --store=xxx --accesskey=xxxx --accesskeyid=xxx  --from="2023-05-05 12:00:00" --sort-by-date --log-type=drds --out=/root/rds.json --threads=4

该进程会一直从sls消费日志数据，可以根据需要手动终止日志下载进程

#### 例如：过滤处理5月5号的日志，只回放5月5号的数据库流量
egrep '^2023-05-05'  /root/rds.json |sort -S 10240M -T /data --parallel=8 rds.json  > /root/out.json

### 从自建MySQL采集分析general log
java  -jar mysqlsniffer.jar --capture-method=general_log  --replay-to=file --port=3306 --username=root --password=xxx  --concurrency=32 --time=60

文件默认输出到 logs/out.json

### 从自建MySQL通过抓包方式采集SQL流量
java  -jar mysqlsniffer.jar --capture-method=net_capture   --replay-to=file --port=3306 --username=root --password=abc123456 --network-device=eth0  --concurrency=32  --time=1000 --log-level=info

文件默认输出到 logs/out.json

### 回放到 mysql
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=mysql --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=test

### 回放到 polarx
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=polarx --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=test

### 参数描述
```
--file          采集的sql文件
--source-db     源库类型 mysql、postgresql、polarx
--replay-to     回放到哪种数据库 mysql、polarx
--host          目标库ip
--port          目标库端口
--username=root 目标库用户名
--password      目标库密码
--database      目标库库名
--concurrency   并发数
--time=1000     执行时间,单位秒
--task          任务名
--schema-map    schema映射和过滤，例如schema1:schema2,schema2,schema3:schema2  只重放schema1、schema2、schema3 3个schema的sql，且schema1和schema3映射到schema2进行重放
--rate-factor   速度控制，1表示原速，0.5表2倍速度，0.1表示10倍速度
--circle        是否循环回放，如果开启循环回放，那边会忽略rate-factor参数，rate-factor置为0，以最大压力回放
--sql-timeout   设置sql超时时间，默认60，单位秒，建议设置合理sql-timeout，避免慢sql影响重放进度
--interval      运行时监控打印时间间隔，默认5，单位秒
--commit	    是否commit，默认frodo会手动开启事务进行回放，执行完一条SQL后会rollback，如果需要commit，可以打开该参数。
--filter        多选：ALL、DQL、DML;  ALL：所有sql；DQL：select；DML：update、insert、delete、replace、merge。参数默认值是ALL。只回放指定的SQL类型
--skip-dupli-error-sql  是否跳过已经报错过的相同SQL指纹的sql，默认关闭，如果打开，一定程度上能够提高重放速度
--disable-insert-to-replace 默认对polarx回放，会把insert转成replace，减少主键冲突的报错，但是replace可能会导致产生死锁。可以设置该参数关闭该特性。
--disable-transaction   是否关闭手动事务，如果关闭，那么SQL使用自动提交模式，--commit参数自动失效

```
