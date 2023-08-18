# README
## usage
### replay to mysql
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=mysql --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=test

### replay to polardb_o
java -Xms=2G -Xmx=4G -jar frodo.jar --file=/root/out.json --source-db=mysql --replay-to=polarx --port=3306 --host=172.25.132.163 --username=root --password=123456 --concurrency=64 --time=1000 --task=task1 --schema-map=test:test1,test2 --log-level=info --rate-factor=1 --database=polardb

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
```