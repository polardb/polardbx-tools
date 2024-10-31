<!-- TOC -->
* [常见导入导出场景](#常见导入导出场景)
  * [数据库表导出](#数据库表导出)
  * [数据库表导入](#数据库表导入)
  * [使用yaml配置](#使用yaml配置)
  * [TPC-H数据集](#TPC-H数据集)
* [常见问题排查](#常见问题排查)
<!-- TOC -->

# 常见使用场景
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

### 整库导出
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

### 指定列导出
`-D sbtest_auto -o export -s , -t "sbtest1" -col "id;k;c"`

### 从单机MySQL中导出数据
`-D sbtest -o export -s , -t "sbtest1" -sharding false`

### 进行数据脱敏
#### 对手机号进行掩码保护
以 TPC-H 数据集的 cusomter 表为例，只展示手机号 c_phone 前三位与末四位
`-D tpch_1g -o export -s , -t "customer" -mask "{
\"c_phone\": { \"type\": \"hiding\", \"show_region\" : \"0-2\", \"show_end\": 4
}"`

**原数据**
```text
c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment
1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag 
2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: care
3|Customer#000000003|MG9kdTD2WBHm|1|11-719-748-3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abo
4|Customer#000000004|XxVSJsLAGtn|4|14-128-190-5944|2866.83|MACHINERY| requests. final, regular ideas sleep final acco
5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-750-942-6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole acco
```

**脱敏后数据**
```text
c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment
1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-********2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag 
2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-********3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: care
3|Customer#000000003|MG9kdTD2WBHm|1|11-********3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abo
4|Customer#000000004|XxVSJsLAGtn|4|14-********5944|2866.83|MACHINERY| requests. final, regular ideas sleep final acco
5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-********6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole acco
```

编写复杂格式的json文件建议参考 [使用yaml配置](#使用yaml配置)。

### 导出到 S3 或 OSS

1. 使用环境变量配置S3的AK、SK、uri：(不支持其他方式传入)
```shell
export S3_ACCESS_KEY_ID=<S3_ACCESS_KEY_ID>
export S3_ACCESS_KEY_SECRET=<S3_ACCESS_KEY_SECRET>
export S3_BUCKET=<S3_BUCKET>
export S3_ENDPOINT=<S3_ENDPOINT>
```

2. 导出命令：`-D sbtest_auto  -t sbtest1 -o export -s , -fs S3 -dir /tmp`

> 1. `-fs S3` 指定文件系统为S3（可以取值为：LOCAL/S3/OSS，默认为LOCAL，即默认导出到本地；此处指定文件系统为S3，则会将文件导出到S3）
> 2. `-dir /tmp` 指定临时文件的生成目录（该参数可为空，默认为当前运行目录），临时文件的大小会根据可用空间自动调整，文件上传完毕后会自动删除
> 3. 如果对象存储上已有同名文件，新导出的文件会覆盖原文件


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

### 导入加密后的文件
解密算法与密钥需要与导出时一致
`-D sbtest_auto -o import -s , -t sbtest1 -enc DEFAULT -key 123456 -f sbtest1_0`

### 导入Excel文件
`-D sbtest_auto -o import -s , -t "sbtest1" -format XLSX -f "sbtest1_0.xlsx"`

### 导入TPC-H数据集

`-D tpch_auto -o import -benchmark TPCH -scale 100`
> 1. 使用 -scale 指定导入数据集的规模，单位：G
> 2. 可以使用 -t "lineitem;orders" 来指定表进行导入

### 更新TPC-H数据集

`-D tpch_auto -o update -benchmark TPCH -scale 100 -F 3`
> 1. 使用 -F 来指定更新的轮数

## 使用yaml配置
当有很多配置项需要设置时，使用命令行参数会很不方便编辑，此时建议使用yaml格式的配置文件，示例如下：

**命令行参数**：`-configFile export.yaml`

**expory.yaml 文件**
```yaml
host: xxxx
port: 3306
user: root
password: xxxxxx
database: tpch_1g
operation: export
sep: "|"
table: customer
filenum: 1
orderby: asc
orderCol: c_custkey
header: true
mask: >-
   {
     "c_phone": {
       "type": "hiding",
       "show_region": "0-2",
       "show_end": 4
     }
   }
```

如果配置值包含[yaml特殊字符](https://yaml.org/spec/1.2.2/#53-indicator-characters)的话， 需要用引号括起来。

## TPC-H数据集

### 导入 TPC-H

`-o import -benchmark tpch -scale ${规格}`

其中规格的单位为GB，比如要导入 TPC-H 100G，则输入`-scale 100`。

根据经验来看，当数据库不成为瓶颈的时候，4C16G的客户端配置可以在30分钟内完成 TPC-H 100G 的导入
（使用参数`-pro 1 -con 80 -ringsize 8192 -minConn 81 -maxConn 81 -batchSize 500`）。

### 更新 TPC-H

` -o update -benchmark tpch -scale ${规格} -F ${更新轮数} `

可以通过设置 并发数 和 BatchSize 来对更新性能进行调优。

### 回滚 TPC-H 更新

` -o delete -benchmark tpch -scale ${规格} -F ${回滚轮数} `

回滚轮数需要与之前更新轮数一致，且确保之前的更新已完整执行

# 常见问题排查
1. 报错 **the server time zone value '' is unrecognized**

   **原因**：由于数据库时区与系统时区有差异导致的报错，需要在jdbc url中手动指定时区

   **解决**：加入参数：`-param "serverTimezone=Asia/Shanghai"`
2. 报错 **Unable to get topology of table** 

   **原因**：批量导出时默认以 PolarDB-X 的物理表拓扑进行分布式导出，
如果对普通 MySQL数据库进行导出，需要关闭 sharding 参数

   **解决**：加入参数：`-sharding false`
3. 数据文件使用的分隔符是tab缩进，需要怎么输入`-s` 参数？

   **解决**：直接在shell中输入tab键，即`-s "	"`

4. 数据文件使用的分隔符是ascii控制字符（如`\x01`等），需要怎么输入`-s` 参数？

   ```text
   1^A123^A1123^A12321312                                                                                                                                                                                                            
   2^A123^A1123^A12321312
   3^A123^A1123^A12321312
   ```
   > ^A 为 \x01 的Caret notation

   **解决**：输入`-s $'\x01'` 即可。
   > 暂时无法处理NULL字符(`\x00`)作为分隔符，可以通过修改源代码解决。

5. 导入时报错，`due to wait millis 5000, active 8, maxActive 8, ...`

   **原因**：由于运行时指定了`-maxConn 8`参数 maxActive连接数是8，而此时active也是8，即连接池满了；
也就是并发导入创建的连接数太多，超出了连接池上限，导致报错

   **解决**：加入 `-con 4`参数限制导入数据库的消费者线程数为4

6. 生产者消费者建议的比例数？

   **原因**：默认情况下，生产者数（并发读取文件线程数）为4，消费者数（并发导入数据库线程数）为CPU核心数的4倍；
   在不同的规格下，默认参数可能表现不佳

   **解决**：通常建议消费者数`-con `为生产者数`-pro `的6~8倍，具体数值还需根据硬件配置、数据库处理能力来调整

7. 报错 `Missing argument for option: `

   **原因**：参数缺少参数值，自1.2.0版本后，开关类型的参数也需要填入参数（true|false）

8. 导出数据时，默认分区并行度较低导致导出耗时较长

   **原因**：默认情况下，导出时分区并行度为常数（4），当数据库实例以及压测机配置较高时，该并行度不足以充分利用硬件资源

   **解决**：调整参数 -pro 的大小来调节导出分区并行度（导出场景中，生产者为连接数据库拉取数据的线程，消费者为文件写入的线程）

9. 导出数据中，字段值包含了分隔符，后续导入时报错 `required field size xx, actual size yy`

   **原因**：由于字段值本身包含了分隔符，导致csv解析时得到的字段数量与表列数匹配不上

   **解决**：导入、导出时都加入参数 `-quote force` 参数（开启该参数后，导入时客户端CPU占用会更高一些，效率也会略低一点，
   但文本解析的兼容性会更好）

10. 导入时报错，`Incorrect value: '\N' for column`

   **原因**：BatchTool 导出NULL值的时候会转义成'\N'字符串

   **解决**：导入、导出时都加入参数 `-quote force` 参数

11. 导入/导出中文字符乱码

   **原因**：可能有多种原因，例如操作系统的编码不是utf8，可能会将中文显示成问号`?`；也可能数据库侧的`character_set_server`
   不是utf8mb4

   **解决**：Linux系统上，可以通过`locale`命令查看编码；
   如果是数据库`character_set_server`变量的问题，BatchTool可以加上`-connParam "useUnicode=true&characterEncoding=utf-8"`

12. 使用`-con`指定了消费者线程数不生效

   **原因**：V1.4.1版本及以前，实际消费者的线程数会设置为`-con`和CPU核数的最大值

   **解决**：可通过`-fcon`参数强制设置消费者线程数；V1.4.2版本开始，已移除`-fcon`参数，会直接使用`-con`指定的数值

13. 导出视图的相关问题

   **原因**：BatchTool 在 v1.4.0 以前的版本，在导出整库时会默认导出所有表和视图的数据，无法控制是否导出视图
   
   **解决**：自v1.4.0开始，可指定参数`-withView true`来导出视图数据

14. 指定目录导入时报错：`No filename with suffix starts with table name...` 或者 `No filename matches table name...`

   **原因**：BatchTool 为了适配自身整库导出的文件名规则，要求文件名必须以表名开头，且文件名后缀满足一定规则（如分区表的分区下标）；
   因此对于包含了自定义文件名导出的目录时，BatchTool 无法对，进而会报错

   **解决**：自v1.4.1开始，导入时可指定参数`--prefix "${文件名前缀}"`来指定目录里与导入表匹配的文件名前缀

15. 导出 PolarDB-X 某张表的指定物理分片

   **解决**：例如某张表有128个物理分片，想导出第0号分片至第63号分片；
   自v1.4.1开始，可指定参数`-part 0:63`来导出第0号分片至第63号分片

16. 导入时，由于一些数据库侧的偶发报错，希望能自动重试

   **原因**：BatchTool 默认情况下，导入失败会直接退出，不会自动重试

**解决**：自v1.4.1开始，可设置参数`-maxError 10`来指定最大错误重试次数为10次，目前暂不支持根据错误码来进行重试

17. 文本数据里面，NULL值的表示是一些特殊的字符或字符串，比如空字符串

    **原因**：BatchTool 默认情况下，是把`\N`作为NULL值的表示

    **解决**：自v1.5.0开始，可设置参数`nullStr ""`来指定导入时，空字符串解析为NULL值