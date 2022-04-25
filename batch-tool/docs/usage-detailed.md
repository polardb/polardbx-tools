
以下省略 `java -jar batch-tool.jar -h 127.0.0.1 -u polardbx_root -p "123456" -P 8527`  等数据库连接信息配置 ，仅展示命令行功能参数的设置

## 数据库表导出
### 单张表默认并行导出
文件数默认为物理表的分片数
`-D sbtest_auto -o export -s , -t sbtest1`
### 单张表指定文件数量导出
-F 1 导出为一个文件，生产者并发度为物理表的分片数，消费者并发度为文件数
`-D sbtest_auto -o export -s , -t sbtest1 -F 1`
### 多表指定文件数量导出
-F 1 两张表将分别导出为两个单独的文件
`-D sbtest_auto -o export -s , -t "sbtest1;sbtest2" -F 1`
### 整库导出		60
-F 1 库中每张表都分别导出为单独的一个文件
`-D sbtest_auto -o export -s , -F 1`
### 元数据导出
导出库的所有建库建表DDL语句
`-D sbtest_auto -o export -s , -DDL only`
### gz压缩导出
`-D sbtest_auto -o export -s , -t "sbtest1" -comp GZIP`
### 加密导出
`-D sbtest_auto -o export -s , -t sbtest1 -enc DEFAULT -key 123456 -F 1`
### 导出为Excel文件
`-D sbtest_auto -o export -s , -t "sbtest1" -format XLSX`

## 数据库表导入
### 单表导入
`-D sbtest_auto -o import -s , -t sbtest2 -dir data-backup`
### 多表导入
`-D sbtest_auto -o import -s , -t "sbtest1;sbtest2" -dir data-backup`
### 整库导入
`-D sbtest_auto -o import -s , -dir data-test`
### 调整导入并行度
生产者并发度为读取文件的工作线程，消费者并发度为发送batch insert的工作线程（两者均可调整）
`-D sbtest_auto -t sbtest1 -o import -s , -f sbtest1_0 -pro 6 -con 6`
### 元数据导入
这里指定的连接库是polardbx(内置默认schema)
`-D polardbx -o import -s , -DDL only -f sbtest_auto.ddl`
### 导入gz压缩文件
`-D sbtest_auto -o import -s , -t "sbtest1" -comp GZIP -dir data-test`
### 导入
解密算法与密钥需要与导出时一致
`-D sbtest_auto -o import -s , -t sbtest1 -enc DEFAULT -key 123456 -f sbtest1_0`
### 导入Excel文件
`-D sbtest_auto -o import -s , -t "sbtest1" -format XLSX -f "sbtest1_0.xlsx"`