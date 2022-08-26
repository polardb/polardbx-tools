/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd;

public class ConfigArgOption {
    protected final String argShort;
    protected final String argLong;
    protected final String desc;
    protected final String argName;

    private ConfigArgOption(String argShort, String argLong, String desc) {
        this(argShort, argLong, desc, null);
    }

    private ConfigArgOption(String argShort, String argLong, String desc, String argName) {
        this.argShort = argShort;
        this.argLong = argLong;
        this.desc = desc;
        this.argName = argName;
    }
    
    private static ConfigArgOption of(String argShort, String argLong, String desc) {
        return new ConfigArgOption(argShort, argLong, desc);
    }

    private static ConfigArgOption of(String argShort, String argLong, String desc, String argName) {
        return new ConfigArgOption(argShort, argLong, desc, argName);
    }


    public static final ConfigArgOption ARG_SHORT_HELP =
        of("help", "help", "Help message.");
    public static final ConfigArgOption ARG_SHORT_VERSION =
        of("v", "version", "Show batch-tool version.");
    public static final ConfigArgOption ARG_SHORT_CONFIG_FILE =
        of("config", "configFile", "Use yaml config file.", "filepath");
    public static final ConfigArgOption ARG_SHORT_USERNAME =
        of("u", "user", "User for login.", "username");
    public static final ConfigArgOption ARG_SHORT_PASSWORD =
        of("p", "password", "Password of user.", "password");
    public static final ConfigArgOption ARG_SHORT_HOST =
        of("h", "host", "Host of database.", "host");
    public static final ConfigArgOption ARG_SHORT_PORT =
        of("P", "port", "Port number of database.", "port");
    public static final ConfigArgOption ARG_SHORT_DBNAME =
        of("D", "database", "Database name.", "database");
    public static final ConfigArgOption ARG_SHORT_LOAD_BALANCE =
        of("lb", "loadbalance",
            "Use jdbc load balance, filling the arg in $host like 'host1:port1,host2:port2'.");
    public static final ConfigArgOption ARG_SHORT_OPERATION =
        of("o", "operation", "Batch operation type: export / import / delete / update.", "operation");
    public static final ConfigArgOption ARG_SHORT_ORDER =
        of("O", "orderby", "Order by type: asc / desc.", "order");
    public static final ConfigArgOption ARG_SHORT_ORDER_COLUMN =
        of("OC", "orderCol", "Ordered column names.", "col1;col2;col3");
    public static final ConfigArgOption ARG_SHORT_COLUMNS =
        of("col", "columns", "Target columns for export.", "col1;col2;col3");
    public static final ConfigArgOption ARG_SHORT_TABLE =
        of("t", "table", "Target table.", "tableName");
    public static final ConfigArgOption ARG_SHORT_SEP =
        of("s", "sep", "Separator between fields (delimiter).", "separator char or string");
    public static final ConfigArgOption ARG_SHORT_PREFIX =
        of("pre", "prefix", "Export file name prefix.", "prefix");
    public static final ConfigArgOption ARG_SHORT_FROM_FILE =
        of("f", "file", "Source file(s).", "filepath1;filepath2");
    public static final ConfigArgOption ARG_SHORT_LINE =
        of("L", "line", "Max line limit of one single export file.", "line count");
    public static final ConfigArgOption ARG_SHORT_FILE_NUM =
        of("F", "filenum", "Fixed number of exported files.", "file count");
    public static final ConfigArgOption ARG_SHORT_HISTORY_FILE =
        of("H", "historyFile", "History file name.", "filepath");
    public static final ConfigArgOption ARG_SHORT_WHERE =
        of("w", "where", "Where condition: col1>99 AND col2<100 ...", "where condition");
    public static final ConfigArgOption ARG_SHORT_ENABLE_SHARDING =
        of("sharding", "sharding", "Whether enable sharding mode.", "ON / OFF");
    public static final ConfigArgOption ARG_SHORT_WITH_HEADER =
        of("header", "header", "Whether the header line is column names (default no).");
    public static final ConfigArgOption ARG_SHORT_DIRECTORY =
        of("dir", "directory", "Directory path including files to import.", "directory path");
    public static final ConfigArgOption ARG_SHORT_CHARSET =
        of("cs", "charset", "The charset of files.", "charset");
    public static final ConfigArgOption ARG_SHORT_IGNORE_AND_RESUME =
        of("i", "ignore", "Flag of insert ignore and resume breakpoint.");
    public static final ConfigArgOption ARG_SHORT_PRODUCER =
        of("pro", "producer", "Configure number of producer threads (export / import).", "producer count");
    public static final ConfigArgOption ARG_SHORT_CONSUMER =
        of("con", "consumer", "Configure number of consumer threads.", "consumer count");
    public static final ConfigArgOption ARG_SHORT_FORCE_CONSUMER =
        of("fcon", "forceConsumer", "Configure if allow force consumer parallelism.", "parallelism");
    public static final ConfigArgOption ARG_SHORT_LOCAL_MERGE =
        of("local", "localMerge", "Use local merge sort.");
    public static final ConfigArgOption ARG_SHORT_SQL_FUNC =
        of("func", "sqlFunc", "Use sql function to update.");
    public static final ConfigArgOption ARG_SHORT_NO_ESCAPE =
        of("noEsc", "noEscape", "Do not escape value for sql.");
    public static final ConfigArgOption ARG_SHORT_MAX_CONN_NUM =
        of("maxConn", "maxConnection", "Max connection count (druid).", "max connection");
    public static final ConfigArgOption ARG_SHORT_MAX_WAIT =
        of("maxWait", "connMaxWait", "Max wait time when getting a connection.", "wait time(ms)");
    public static final ConfigArgOption ARG_SHORT_MIN_CONN_NUM =
        of("minConn", "minConnection", "Min connection count (druid).", "min connection");
    public static final ConfigArgOption ARG_SHORT_CONN_PARAM =
        of("param", "connParam", "Jdbc connection params.", "key1=val1&key2=val2");
    public static final ConfigArgOption ARG_SHORT_CONN_INIT_SQL =
        of("initSqls", "initSqls", "Connection init sqls (druid).", "sqls");
    public static final ConfigArgOption ARG_SHORT_BATCH_SIZE =
        of("batchsize", "batchSize", "Batch size of insert.", "size");
    public static final ConfigArgOption ARG_SHORT_READ_BLOCK_SIZE =
        of("readsize", "readSize", "Read block size.", "size(MB)");
    public static final ConfigArgOption ARG_SHORT_RING_BUFFER_SIZE =
        of("ringsize", "ringSize", "Ring buffer size.", "size (power of 2)");
    public static final ConfigArgOption ARG_SHORT_READ_FILE_ONLY =
        of("rfonly", "readFileOnly", "Only read and process file, no sql execution.");
    public static final ConfigArgOption ARG_SHORT_USING_IN =
        of("in", "whereIn", "Using where cols in (values).");
    public static final ConfigArgOption ARG_SHORT_WITH_LAST_SEP =
        of("lastSep", "withLastSep", "Whether line ends with separator.");
    public static final ConfigArgOption ARG_SHORT_PARALLEL_MERGE =
        of("para", "paraMerge", "Use parallel merge when doing order by export.");
    public static final ConfigArgOption ARG_SHORT_QUOTE_ENCLOSE_MODE =
        of("quote", "quoteMode",
            "The mode of how field values are enclosed by double-quotes when exporting table.",
            "AUTO (default) / FORCE / NONE");
    public static final ConfigArgOption ARG_SHORT_TPS_LIMIT =
        of("tps", "tpsLimit", "Configure of tps limit (default -1: no limit).", "tps limit");
    public static final ConfigArgOption ARG_SHORT_WITH_DDL =
        of("DDL", "DDL", "Export or import with DDL sql mode.", "NONE (default) / ONLY / WITH");
    public static final ConfigArgOption ARG_SHORT_COMPRESS =
        of("comp", "compress", "Export or import compressed file.", "NONE (default) / GZIP");
    public static final ConfigArgOption ARG_SHORT_ENCRYPTION =
        of("encrypt", "encrypt", "Export or import with encrypted file.", "NONE (default) / AES / SM4");
    public static final ConfigArgOption ARG_SHORT_KEY =
        of("key", "secretKey", "Secret key used during encryption.", "string-type key");
    public static final ConfigArgOption ARG_SHORT_FILE_FORMAT =
        of("format", "fileFormat", "File format.", "NONE (default) / TXT / CSV / XLS / XLSX");
    public static final ConfigArgOption ARG_SHORT_MAX_ERROR =
        of("error", "maxError", "Max error count threshold, program exits when the limit is exceeded.", "max error count");
    public static final ConfigArgOption ARG_SHORT_PERF_MODE =
        of("perf", "perfMode", "Use performance mode (at the sacrifice of compatibility.)");
    public static final ConfigArgOption ARG_SHORT_MASK =
        of("mask", "mask", "Masking sensitive columns while exporting data.", "Json format config");

    public boolean hasArg() {
        return argName != null;
    }
}
