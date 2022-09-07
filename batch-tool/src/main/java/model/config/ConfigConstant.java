/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package model.config;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ConfigConstant {

    public static final String APP_NAME = "BatchTool";

    public static final int CPU_NUM = Runtime.getRuntime().availableProcessors();
    /**
     * 默认分隔符
     */
    public static final String DEFAULT_SEPARATOR = ",";

    /**
     * \x00
     */
    public static final String NULL_SEPARATOR = "\u0000";

    /**
     * 文件名/表名分隔符
     */
    public static final String CMD_SEPARATOR = ";";

    /**
     * 文件名起始行号分隔符
     */
    public static final String CMD_FILE_LINE_SEPARATOR = ":";

    /**
     * 默认引号转义模式
     */
    public static final QuoteEncloseMode DEFAULT_QUOTE_ENCLOSE_MODE = QuoteEncloseMode.AUTO;

    public static final int DEFAULT_READ_BLOCK_SIZE_IN_MB = 2;

    /**
     * 默认读取文件的生产者数量
     */
    public static final int DEFAULT_PRODUCER_SIZE = 4;
    /**
     * 默认消费者数量
     */
    public static final int DEFAULT_CONSUMER_SIZE = CPU_NUM * 4;

    /**
     * 默认不强制使用指定并发度，考虑实际机器的core数目
     */
    public static final boolean DEFAULT_FORCE_CONSUMER_PARALLELISM = false;

    /**
     * 默认tps limit为-1，指不限流
     */
    public static final int DEFAULT_TPS_LIMIT = -1;

    /**
     * 默认存储历史数据的文档
     */
    public static final String DEFAULT_HISTORY_FILE = "history_file";

    /**
     * DDL语句的文件名后缀
     */
    public static final String DDL_FILE_SUFFIX = ".ddl";

    /**
     * 更新时的整数倍数
     */
    public static final int INT_UPDATE_MULTIPLICAND = 2;

    /**
     * 更新时的浮点数倍数
     */
    public static final float FLOAT_UPDATE_MULTIPLICAND = 2.0f;

    public static final String DEFAULT_SCHEMA_NAME = "polardbx";

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public static final CompressMode DEFAULT_COMPRESS_MODE = CompressMode.NONE;

    public static final EncryptionConfig DEFAULT_ENCRYPTION_CONFIG = EncryptionConfig.NONE;

    public static final FileFormat DEFAULT_FILE_FORMAT = FileFormat.NONE;

    /**
     * 遇到错误即退出
     */
    public static final int DEFAULT_MAX_ERROR_COUNT = 0;

    public static final String BROKEN_LINE_FILE_NAME = "err-data";

    public static final String ORDER_BY_TYPE_ASC = "asc";

    public static final String ORDER_BY_TYPE_DESC = "desc";

    public static final String END_OF_BATCH_LINES = "END_OF_BATCH_LINES";

    /**
     * 64KB
     */
    public static final int DEFAULT_COMPRESS_BUFFER_SIZE = 64 * 1024;

    /**
     * OpenCSV库不支持直接读取一行 需读取出字段再用该魔法值拼接
     * FIXME
     */
    public static final String MAGIC_CSV_SEP1 = "|@|";
    public static final String MAGIC_CSV_SEP2 = "^@^";

    /**
     * 仅导出时默认开启基于分库分表的模式
     */
    public static final boolean DEFAULT_EXPORT_SHARDING_ENABLED = true;
    public static final boolean DEFAULT_IMPORT_SHARDING_ENABLED = false;

    public static final List<String> ILLEGAL_SEPARATORS = new ArrayList<String>() {{
        add("\"");
        add("\\");
        add(MAGIC_CSV_SEP1);
    }};
}
