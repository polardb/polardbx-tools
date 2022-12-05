# 常见导入导出场景
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
