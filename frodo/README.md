# README
## 打包
sh build.sh

## usage
### 从sls采集drds或rds日志
java -jar slssniffer-1.0.jar --endpoint=cn-hangzhou-intranet.log.aliyuncs.com --project=xxx  --store=xxx --accesskey=xxxx --accesskeyid=xxx  --from="2023-05-05 12:00:00" --sort-by-date --log-type=drds --out=/root/rds.json --threads=4

#### 过滤处理5月5号的日志
egrep '^2023-05-05'  /root/rds.json |sort -S 10240M -T /data --parallel=8 rds.json  > /root/out.json

### 从自建mysql采集general 日志
java  -jar mysqlsniffer.jar --capture-method=general_log  --replay-to=file --port=3306 --username=root --password=xxx  --concurrency=32 --time=60
文件默认输出到 logs/out.json

### replay to mysql
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=mysql --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=test

### replay to polarx
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=polarx --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=test

### replay to polardb_o
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=polardb_o --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=polardb

### 参数描述
```
--file          采集的sql文件
--source-db     源库类型 oracle、mysql、db2
--replay-to     回放到哪种数据库 mysql、polardb_o
--host          目标库ip
--port          目标库端口
--username=root 目标库用户名
--password      目标库密码
--database      目标库库名
--concurrency   并发数
--time=1000     执行时间
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