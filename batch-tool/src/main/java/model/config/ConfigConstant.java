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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ConfigConstant {

    public static final String APP_NAME = "BatchTool";

    public static final String ARG_SHORT_HELP = "help";
    public static final String ARG_SHORT_VERSION = "v";
    public static final String ARG_SHORT_PORT = "P";
    public static final String ARG_SHORT_USERNAME = "u";
    public static final String ARG_SHORT_PASSWORD = "p";
    public static final String ARG_SHORT_HOST = "h";
    public static final String ARG_SHORT_DBNAME = "D";
    public static final String ARG_SHORT_LOAD_BALANCE = "lb";

    public static final String ARG_SHORT_OPERATION = "o";
    public static final String ARG_SHORT_ORDER = "O";
    public static final String ARG_SHORT_ORDER_COLUMN = "OC";
    public static final String ARG_SHORT_TABLE = "t";
    public static final String ARG_SHORT_SEP = "s";
    public static final String ARG_SHORT_PREFIX = "pre";
    public static final String ARG_SHORT_FROM = "f";
    public static final String ARG_SHORT_LINE = "L";
    public static final String ARG_SHORT_FILE_NUM = "F";
    public static final String ARG_SHORT_HISTORY_FILE = "H";
    public static final String ARG_SHORT_WHERE = "w";
    public static final String ARG_SHORT_ENABLE_SHARDING = "sharding";
    public static final String ARG_SHORT_WITH_HEADER = "header";
    public static final String ARG_SHORT_DIRECTORY = "dir";
    public static final String ARG_SHORT_CHARSET = "cs";
    public static final String ARG_SHORT_IGNORE_AND_RESUME = "i";
    public static final String ARG_SHORT_PRODUCER = "pro";
    public static final String ARG_SHORT_CONSUMER = "con";
    public static final String ARG_SHORT_FORCE_CONSUMER = "fcon";
    public static final String ARG_SHORT_LOCAL_MERGE = "local";
    public static final String ARG_SHORT_SQL_FUNC = "func";
    public static final String ARG_SHORT_NO_ESCAPE = "noesc";
    public static final String ARG_SHORT_MAX_CONN_NUM = "maxConn";
    public static final String ARG_SHORT_MAX_WAIT = "maxWait";
    public static final String ARG_SHORT_MIN_CONN_NUM = "minConn";
    public static final String ARG_SHORT_CONN_PARAM = "param";
    public static final String ARG_SHORT_CONN_INIT_SQL = "initSqls";
    public static final String ARG_SHORT_BATCH_SIZE = "batchsize";
    public static final String ARG_SHORT_READ_BLOCK_SIZE = "readsize";
    public static final String ARG_SHORT_RING_BUFFER_SIZE = "ringsize";
    public static final String ARG_SHORT_READ_FILE_ONLY = "rfonly";
    public static final String ARG_SHORT_USING_IN = "in";
    public static final String ARG_SHORT_WITH_LAST_SEP = "lastSep";
    public static final String ARG_SHORT_PARALLEL_MERGE = "para";
    public static final String ARG_SHORT_QUOTE_ENCLOSE_MODE = "quote";
    public static final String ARG_SHORT_TPS_LIMIT = "tps";

    public static final int CPU_NUM = Runtime.getRuntime().availableProcessors();
    /**
     * 默认分隔符
     */
    public static final String DEFAULT_SEPARATOR = ",";

    /**
     * 文件名分隔符
     */
    public static final String CMD_FILENAME_SEPARATOR = ";";

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
     * 更新时的整数倍数
     */
    public static final int INT_UPDATE_MULTIPLICAND = 2;

    /**
     * 更新时的浮点数倍数
     */
    public static final float FLOAT_UPDATE_MULTIPLICAND = 2.0f;

    /**
     * 更新时的浮点数倍数(精确)
     */
    public static final BigDecimal DECIMAL_UPDATE_MULTIPLICAND = BigDecimal.valueOf(2);

    public static final String DEFAULT_CHARSET = "utf-8";

    public static final boolean DEFAULT_WITH_HEADER = false;

    public static final String BROKEN_LINE_FILE_NAME = "err-data";

    public static final String ORDER_BY_TYPE_ASC = "asc";

    public static final String ORDER_BY_TYPE_DESC = "desc";

    public static final String END_OF_BATCH_LINES = "END_OF_BATCH_LINES";

    /**
     * OpenCSV库不支持直接读取一行 需读取出字段再用该魔法值拼接
     * FIXME
     */
    public static final String MAGIC_CSV_SEP = "|@|";

    /**
     * 仅导出时默认开启基于分库分表的模式
     */
    public static final boolean DEFAULT_EXPORT_SHARDING_ENABLED = true;
    public static final boolean DEFAULT_IMPORT_SHARDING_ENABLED = false;

    public static final List<String> ILLEGAL_SEPARATORS = new ArrayList<String>() {{
        add("\"");
        add("\\");
        add(MAGIC_CSV_SEP);
    }};
}
