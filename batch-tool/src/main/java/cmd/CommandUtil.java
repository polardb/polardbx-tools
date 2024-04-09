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

package cmd;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import datasource.DataSourceConfig;
import datasource.DatasourceConstant;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.BenchmarkMode;
import model.config.CompressMode;
import model.config.ConfigConstant;
import model.config.DdlMode;
import model.config.EncryptionConfig;
import model.config.ExportConfig;
import model.config.FileFormat;
import model.config.FileLineRecord;
import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;
import util.Version;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cmd.ConfigArgOption.ARG_DDL_PARALLELISM;
import static cmd.ConfigArgOption.ARG_DDL_RETRY_COUNT;
import static cmd.ConfigArgOption.ARG_SHORT_BATCH_SIZE;
import static cmd.ConfigArgOption.ARG_SHORT_BENCHMARK;
import static cmd.ConfigArgOption.ARG_SHORT_CHARSET;
import static cmd.ConfigArgOption.ARG_SHORT_COLUMNS;
import static cmd.ConfigArgOption.ARG_SHORT_COMPRESS;
import static cmd.ConfigArgOption.ARG_SHORT_CONFIG_FILE;
import static cmd.ConfigArgOption.ARG_SHORT_CONN_INIT_SQL;
import static cmd.ConfigArgOption.ARG_SHORT_CONN_PARAM;
import static cmd.ConfigArgOption.ARG_SHORT_CONSUMER;
import static cmd.ConfigArgOption.ARG_SHORT_DBNAME;
import static cmd.ConfigArgOption.ARG_SHORT_DIRECTORY;
import static cmd.ConfigArgOption.ARG_SHORT_ENCRYPTION;
import static cmd.ConfigArgOption.ARG_SHORT_FILE_FORMAT;
import static cmd.ConfigArgOption.ARG_SHORT_FILE_NUM;
import static cmd.ConfigArgOption.ARG_SHORT_FORCE_CONSUMER;
import static cmd.ConfigArgOption.ARG_SHORT_FROM_FILE;
import static cmd.ConfigArgOption.ARG_SHORT_HELP;
import static cmd.ConfigArgOption.ARG_SHORT_HISTORY_FILE;
import static cmd.ConfigArgOption.ARG_SHORT_HOST;
import static cmd.ConfigArgOption.ARG_SHORT_KEY;
import static cmd.ConfigArgOption.ARG_SHORT_LINE;
import static cmd.ConfigArgOption.ARG_SHORT_MASK;
import static cmd.ConfigArgOption.ARG_SHORT_MAX_CONN_NUM;
import static cmd.ConfigArgOption.ARG_SHORT_MAX_ERROR;
import static cmd.ConfigArgOption.ARG_SHORT_MAX_WAIT;
import static cmd.ConfigArgOption.ARG_SHORT_MIN_CONN_NUM;
import static cmd.ConfigArgOption.ARG_SHORT_OPERATION;
import static cmd.ConfigArgOption.ARG_SHORT_ORDER;
import static cmd.ConfigArgOption.ARG_SHORT_ORDER_COLUMN;
import static cmd.ConfigArgOption.ARG_SHORT_PASSWORD;
import static cmd.ConfigArgOption.ARG_SHORT_PORT;
import static cmd.ConfigArgOption.ARG_SHORT_PREFIX;
import static cmd.ConfigArgOption.ARG_SHORT_PRODUCER;
import static cmd.ConfigArgOption.ARG_SHORT_QUOTE_ENCLOSE_MODE;
import static cmd.ConfigArgOption.ARG_SHORT_READ_BLOCK_SIZE;
import static cmd.ConfigArgOption.ARG_SHORT_RING_BUFFER_SIZE;
import static cmd.ConfigArgOption.ARG_SHORT_SCALE;
import static cmd.ConfigArgOption.ARG_SHORT_SEP;
import static cmd.ConfigArgOption.ARG_SHORT_TABLE;
import static cmd.ConfigArgOption.ARG_SHORT_TPS_LIMIT;
import static cmd.ConfigArgOption.ARG_SHORT_USERNAME;
import static cmd.ConfigArgOption.ARG_SHORT_VERSION;
import static cmd.ConfigArgOption.ARG_SHORT_WHERE;
import static cmd.ConfigArgOption.ARG_SHORT_WITH_DDL;
import static cmd.FlagOption.ARG_DROP_TABLE_IF_EXISTS;
import static cmd.FlagOption.ARG_EMPTY_AS_NULL;
import static cmd.FlagOption.ARG_SHORT_ENABLE_SHARDING;
import static cmd.FlagOption.ARG_SHORT_IGNORE_AND_RESUME;
import static cmd.FlagOption.ARG_SHORT_LOAD_BALANCE;
import static cmd.FlagOption.ARG_SHORT_LOCAL_MERGE;
import static cmd.FlagOption.ARG_SHORT_NO_ESCAPE;
import static cmd.FlagOption.ARG_SHORT_PARALLEL_MERGE;
import static cmd.FlagOption.ARG_SHORT_PERF_MODE;
import static cmd.FlagOption.ARG_SHORT_READ_FILE_ONLY;
import static cmd.FlagOption.ARG_SHORT_SQL_FUNC;
import static cmd.FlagOption.ARG_SHORT_USING_IN;
import static cmd.FlagOption.ARG_SHORT_WITH_HEADER;
import static cmd.FlagOption.ARG_SHORT_WITH_LAST_SEP;
import static cmd.FlagOption.ARG_TRIM_RIGHT;
import static cmd.FlagOption.ARG_WITH_VIEW;

/**
 * 从命令行输入解析配置
 */
public class CommandUtil {
    private static final Logger logger = LoggerFactory.getLogger(CommandUtil.class);

    private static final HelpFormatter formatter = new HelpFormatter();
    private static final Options options = new Options();

    static {
        formatter.setWidth(110);
        addCommandOptions(ConfigArgOption.class);
        addCommandOptions(FlagOption.class);
    }

    private static void addCommandOptions(Class<? extends ConfigArgOption> clazz) {
        Field[] fields = clazz.getFields();
        try {
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())
                    && field.getType() == clazz) {
                    ConfigArgOption option = (ConfigArgOption) field.get(clazz);
                    addConfigOption(option);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void addConfigOption(ConfigArgOption option) {
        Option.Builder builder = Option.builder(option.argShort)
            .longOpt(option.argLong)
            .desc(option.desc);
        if (option.hasArg()) {
            builder.hasArg().argName(option.argName);
        }

        options.addOption(builder.build());
    }

    /**
     * 解析程序启动参数
     */
    public static ConfigResult parseStartUpCommand(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            // 开始解析命令行参数
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            printHelp();
            return null;
        }

        if (commandLine.hasOption(ARG_SHORT_CONFIG_FILE.argShort)) {
            return new YamlConfigResult(commandLine.getOptionValue(ARG_SHORT_CONFIG_FILE.argShort), commandLine);
        } else {
            return new CommandLineConfigResult(commandLine);
        }
    }

    //region 数据源设置
    private static void validateDataSourceArgs(ConfigResult result) {
        requireArg(result, ARG_SHORT_HOST);
        requireArg(result, ARG_SHORT_USERNAME);
        requireArg(result, ARG_SHORT_PASSWORD);
        requireArg(result, ARG_SHORT_DBNAME);
    }

    public static DataSourceConfig getDataSourceConfigFromCmd(ConfigResult result) {
        validateDataSourceArgs(result);

        // 判断是否使用负载均衡方式访问
        DataSourceConfig.DataSourceConfigBuilder configBuilder =
            new DataSourceConfig.DataSourceConfigBuilder();
        configBuilder.host(result.getOptionValue(ARG_SHORT_HOST))
            .dbName(result.getOptionValue(ARG_SHORT_DBNAME))
            .username(result.getOptionValue(ARG_SHORT_USERNAME))
            .password(result.getOptionValue(ARG_SHORT_PASSWORD))
            .maxConnNumber(getMaxConnNum(result))
            .minConnNumber(getMinConnNum(result))
            .maxWait(getMaxWait(result))
            .connParam(getConnParam(result))
            .initSqls(getInitSqls(result));

        if (getLoadBalance(result)) {
            configBuilder.loadBalanceEnabled(true);
        } else {
            configBuilder.port(result.getOptionValue(ARG_SHORT_PORT))
                .loadBalanceEnabled(false);
        }
        return configBuilder.build();
    }

    private static boolean getLoadBalance(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_LOAD_BALANCE);
    }

    private static int getMaxWait(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_MAX_WAIT)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_WAIT));
        } else {
            return DatasourceConstant.MAX_WAIT_TIME;
        }
    }

    private static int getMaxConnNum(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_MAX_CONN_NUM)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_CONN_NUM));
        } else {
            return DatasourceConstant.MAX_CONN_NUM;
        }
    }

    private static int getMinConnNum(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_MIN_CONN_NUM)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MIN_CONN_NUM));
        } else {
            int pro = getProducerParallelism(result);
            int con = getConsumerParallelism(result);
            int maxParallelism = Math.max(pro, con);
            return Math.min(maxParallelism + 1, DatasourceConstant.MIN_CONN_NUM);
        }
    }

    private static String getConnParam(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_CONN_PARAM)) {
            return result.getOptionValue(ARG_SHORT_CONN_PARAM);
        }
        return null;
    }

    private static String getInitSqls(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_CONN_INIT_SQL)) {
            return result.getOptionValue(ARG_SHORT_CONN_INIT_SQL);
        }
        return null;
    }
    //endregion 数据源设置

    //region 批处理命令解析
    public static BaseOperateCommand getOperateCommandFromCmd(ConfigResult result) {
        validateOperateArgs(result);
        BaseOperateCommand command = initCommand(result);
        afterInitCommand(command, result);
        return command;
    }

    private static void validateOperateArgs(ConfigResult result) {
        requireArg(result, ARG_SHORT_OPERATION);
        requireArg(result, ARG_SHORT_DBNAME);
    }

    private static BaseOperateCommand initCommand(ConfigResult result) {
        // 获取命令类型
        String commandTypeStr = result.getOptionValue(ARG_SHORT_OPERATION);
        CommandType commandType = CommandUtil.lookup(commandTypeStr);
        BaseOperateCommand command = null;
        switch (commandType) {
        case EXPORT:
            command = parseExportCommand(result);
            break;
        case IMPORT:
            command = parseImportCommand(result);
            break;
        case UPDATE:
            command = parseUpdateCommand(result);
            break;
        case DELETE:
            command = parseDeleteCommand(result);
            break;
        default:
            throw new IllegalArgumentException("Unsupported command: " + commandTypeStr);
        }
        command.setTableNamesInCmd(getTableNames(result));
        command.setColumnNames(getColumnNames(result));
        return command;
    }

    private static void afterInitCommand(BaseOperateCommand command, ConfigResult result) {
        if (result.hasOption(ARG_SHORT_ENABLE_SHARDING)) {
            command.setShardingEnabled(result.getBooleanFlag(ARG_SHORT_ENABLE_SHARDING));
        }
    }

    private static List<String> getTableNames(ConfigResult result) {
        if (!result.hasOption(ARG_SHORT_TABLE)) {
            return null;
        }
        String tableNameStr = result.getOptionValue(ARG_SHORT_TABLE);
        return Lists.newArrayList(
            StringUtils.split(tableNameStr, ConfigConstant.CMD_SEPARATOR));
    }

    private static List<String> getColumnNames(ConfigResult result) {
        if (!result.hasOption(ARG_SHORT_COLUMNS)) {
            return null;
        }
        String columnNameStr = result.getOptionValue(ARG_SHORT_COLUMNS);
        return Lists.newArrayList(
            StringUtils.split(columnNameStr, ConfigConstant.CMD_SEPARATOR));
    }

    private static BaseOperateCommand parseImportCommand(ConfigResult result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM_FILE, ARG_SHORT_DIRECTORY, ARG_SHORT_BENCHMARK);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        return new ImportCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }

    private static BaseOperateCommand parseDeleteCommand(ConfigResult result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM_FILE, ARG_SHORT_DIRECTORY, ARG_SHORT_BENCHMARK);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        if (producerExecutionContext.getBenchmarkMode() == BenchmarkMode.TPCH) {
            setUpdateBatchSize(result);
        }

        consumerExecutionContext.setWhereCondition(getWhereCondition(result));
        return new DeleteCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }

    private static BaseOperateCommand parseUpdateCommand(ConfigResult result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM_FILE, ARG_SHORT_DIRECTORY, ARG_SHORT_BENCHMARK);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        if (producerExecutionContext.getBenchmarkMode() == BenchmarkMode.TPCH) {
            setUpdateBatchSize(result);
        }

        consumerExecutionContext.setWhereCondition(getWhereCondition(result));
        consumerExecutionContext.setFuncSqlForUpdateEnabled(getFuncEnabled(result));
        return new UpdateCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }
    //endregion 批处理命令解析

    //region 读写文件相关配置
    private static String getSep(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_SEP)) {
            return result.getOptionValue(ARG_SHORT_SEP);
        }
        return ConfigConstant.DEFAULT_SEPARATOR;
    }

    private static Charset getCharset(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_CHARSET)) {
            String charset = result.getOptionValue(ARG_SHORT_CHARSET);
            return Charset.forName(charset);
        }
        return ConfigConstant.DEFAULT_CHARSET;
    }

    private static boolean getWithHeader(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_WITH_HEADER);
    }

    private static CompressMode getCompressMode(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_COMPRESS)) {
            return CompressMode.fromString(result.getOptionValue(ARG_SHORT_COMPRESS));
        }
        return ConfigConstant.DEFAULT_COMPRESS_MODE;
    }

    private static EncryptionConfig getEncryptionConfig(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_ENCRYPTION)) {
            String encryptionMode = result.getOptionValue(ARG_SHORT_ENCRYPTION);
            String key = result.getOptionValue(ARG_SHORT_KEY);
            return EncryptionConfig.parse(encryptionMode, key);
        }
        return ConfigConstant.DEFAULT_ENCRYPTION_CONFIG;
    }

    private static int getReadBlockSizeInMb(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_READ_BLOCK_SIZE)) {
            return Integer.parseInt(
                result.getOptionValue(ARG_SHORT_READ_BLOCK_SIZE));
        }
        return ConfigConstant.DEFAULT_READ_BLOCK_SIZE_IN_MB;
    }

    private static boolean getWithLastSep(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_WITH_LAST_SEP);
    }

    private static boolean getWithView(ConfigResult result) {
        return result.getBooleanFlag(ARG_WITH_VIEW);
    }

    private static boolean getEmptyAsNull(ConfigResult result) {
        return result.getBooleanFlag(ARG_EMPTY_AS_NULL);
    }

    private static boolean getDropTableIfExist(ConfigResult result) {
        return result.getBooleanFlag(ARG_DROP_TABLE_IF_EXISTS);
    }

    private static FileFormat getFileFormat(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_FILE_FORMAT)) {
            String fileFormat = result.getOptionValue(ARG_SHORT_FILE_FORMAT);
            return FileFormat.fromString(fileFormat);
        }
        return ConfigConstant.DEFAULT_FILE_FORMAT;
    }
    //endregion 读写文件相关配置

    //region 导出相关设置
    private static ExportCommand parseExportCommand(ConfigResult result) {
        List<String> tableNames = getTableNames(result);
        ExportConfig exportConfig = new ExportConfig();
        exportConfig.setCharset(getCharset(result));
        exportConfig.setWithHeader(getWithHeader(result));
        exportConfig.setSeparator(getSep(result));
        exportConfig.setWhereCondition(getWhereCondition(result));
        exportConfig.setDdlMode(getDdlMode(result));
        if (exportConfig.getDdlMode() != DdlMode.NO_DDL) {
            setGlobalDdlConfig(result);
        }
        exportConfig.setDropTableIfExists(getDropTableIfExist(result));
        exportConfig.setEncryptionConfig(getEncryptionConfig(result));
        exportConfig.setFileFormat(getFileFormat(result));
        exportConfig.setCompressMode(getCompressMode(result));
        exportConfig.setParallelism(getProducerParallelism(result));
        exportConfig.setQuoteEncloseMode(getQuoteEncloseMode(result));
        exportConfig.setWithLastSep(getWithLastSep(result));
        exportConfig.setWithView(getWithView(result));
        setDir(result, exportConfig);
        setFilenamePrefix(result, exportConfig);
        setFileNum(result, exportConfig);
        setFileLine(result, exportConfig);
        setOrderBy(result, exportConfig);
        setColumnMaskerMap(result, exportConfig);
        exportConfig.validate();
        return new ExportCommand(getDbName(result), tableNames, exportConfig);
    }

    private static void setDir(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_DIRECTORY)) {
            String dirPath = result.getOptionValue(ARG_SHORT_DIRECTORY);
            File file = new File(dirPath);
            FileUtil.checkWritableDir(file);
            try {
                exportConfig.setPath(file.getCanonicalPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void setFilenamePrefix(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_PREFIX)) {
            exportConfig.setFilenamePrefix(result.getOptionValue(ARG_SHORT_PREFIX));
        } else {
            exportConfig.setFilenamePrefix("");
        }
    }

    private static void setOrderBy(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_ORDER)) {
            if (!result.hasOption(ARG_SHORT_ORDER_COLUMN)) {
                throw new IllegalArgumentException("Order column name cannot be empty");
            }
            if (result.getBooleanFlag(ARG_SHORT_LOCAL_MERGE)) {
                exportConfig.setLocalMerge(true);
            }
            exportConfig
                .setAscending(!ConfigConstant.ORDER_BY_TYPE_DESC.equals(result.getOptionValue(ARG_SHORT_ORDER)));
            List<String> columnNameList = Arrays.asList(StringUtils.split(result.getOptionValue(ARG_SHORT_ORDER_COLUMN),
                ConfigConstant.CMD_SEPARATOR));
            exportConfig.setOrderByColumnNameList(columnNameList);
            exportConfig.setParallelMerge(getParaMerge(result));
        }
    }

    private static void setColumnMaskerMap(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_MASK)) {
            String maskConfigStr = result.getOptionValue(ARG_SHORT_MASK);
            JSONObject maskConfig;
            try {
                maskConfig = JSONObject.parseObject(maskConfigStr);
            } catch (JSONException e) {
                throw new IllegalArgumentException("Illegal json format: " + maskConfigStr);
            }
            Map<String, JSONObject> columnMaskerMap = new HashMap<>();
            for (String column : maskConfig.keySet()) {
                JSONObject jsonConfig = maskConfig.getJSONObject(column);
                columnMaskerMap.put(column, jsonConfig);
            }
            exportConfig.setColumnMaskerConfigMap(columnMaskerMap);
        }
    }

    private static void setFileLine(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_LINE)) {
            if (exportConfig.getExportWay() != ExportConfig.ExportWay.DEFAULT) {
                // 只能指定一个导出方式
                throw new IllegalArgumentException("Export way should be unique");
            }
            exportConfig.setExportWay(ExportConfig.ExportWay.MAX_LINE_NUM_IN_SINGLE_FILE);
            exportConfig.setMaxLine(Integer.parseInt(result.getOptionValue(ARG_SHORT_LINE)));
        }
    }

    private static void setFileNum(ConfigResult result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_FILE_NUM)) {
            exportConfig.setExportWay(ExportConfig.ExportWay.FIXED_FILE_NUM);
            exportConfig.setFixedFileNum(Integer.parseInt(result.getOptionValue(ARG_SHORT_FILE_NUM)));
            if (exportConfig.getLimitNum() <= 0) {
                throw new IllegalArgumentException("File num should be a positive integer");
            }
        }
    }

    private static boolean getParaMerge(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_PARALLEL_MERGE);
    }
    //endregion 导出相关设置

    //region 写入数据库操作的设置
    /**
     * 主要针对插入/更新/删除
     * 配置公共的上下文执行环境
     */
    private static void configureCommonContext(ConfigResult result,
                                               ProducerExecutionContext producerExecutionContext,
                                               ConsumerExecutionContext consumerExecutionContext) {
        configureGlobalVar(result);
        configureProducerContext(result, producerExecutionContext);
        configureConsumerContext(result, consumerExecutionContext);
    }

    /**
     * 设置全局可调参数
     */
    private static void configureGlobalVar(ConfigResult result) {
        setBatchSize(result);
        setRingBufferSize(result);
        setPerfMode(result);
    }

    private static void setUpdateBatchSize(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_BATCH_SIZE)) {
            GlobalVar.setTpchUpdateBatchSize(Integer.parseInt(result.getOptionValue(ARG_SHORT_BATCH_SIZE)));
        }
    }

    /**
     * 配置生产者
     */
    private static void configureProducerContext(ConfigResult result,
                                                 ProducerExecutionContext producerExecutionContext) {
        producerExecutionContext.setCharset(getCharset(result));
        producerExecutionContext.setSeparator(getSep(result));
        producerExecutionContext.setDdlMode(getDdlMode(result));

        if (producerExecutionContext.getDdlMode() != DdlMode.DDL_ONLY) {
            producerExecutionContext.setDataFileLineRecordList(getDataFileRecordList(result));
        }
        if (producerExecutionContext.getDdlMode() != DdlMode.NO_DDL) {
            producerExecutionContext.setDdlFileLineRecordList(getDdlFileRecordList(result));
        }
        producerExecutionContext.setParallelism(getProducerParallelism(result));
        producerExecutionContext.setReadBlockSizeInMb(getReadBlockSizeInMb(result));
        producerExecutionContext.setWithHeader(getWithHeader(result));
        producerExecutionContext.setWithView(getWithView(result));
        producerExecutionContext.setCompressMode(getCompressMode(result));
        producerExecutionContext.setEncryptionConfig(getEncryptionConfig(result));
        producerExecutionContext.setFileFormat(getFileFormat(result));
        producerExecutionContext.setMaxErrorCount(getMaxErrorCount(result));
        producerExecutionContext.setHistoryFileAndParse(getHistoryFile(result));
        producerExecutionContext.setQuoteEncloseMode(getQuoteEncloseMode(result));
        producerExecutionContext.setTrimRight(getTrimRight(result));
        producerExecutionContext.setBenchmarkMode(getBenchmarkMode(result));
        producerExecutionContext.setBenchmarkRound(getBenchmarkRound(result));
        producerExecutionContext.setScale(getScale(result));

        producerExecutionContext.validate();
    }

    /**
     * 配置消费者
     */
    private static void configureConsumerContext(ConfigResult result,
                                                 ConsumerExecutionContext consumerExecutionContext) {
        consumerExecutionContext.setCharset(getCharset(result));
        consumerExecutionContext.setSeparator(getSep(result));
        consumerExecutionContext.setInsertIgnoreAndResumeEnabled(getInsertIgnoreAndResumeEnabled(result));
        consumerExecutionContext.setParallelism(getConsumerParallelism(result));
        consumerExecutionContext.setForceParallelism(getForceParallelism(result));
        consumerExecutionContext.setTableNames(getTableNames(result));
        consumerExecutionContext.setSqlEscapeEnabled(getSqlEscapeEnabled(result));
        consumerExecutionContext.setReadProcessFileOnly(getReadAndProcessFileOnly(result));
        consumerExecutionContext.setWhereInEnabled(getWhereInEnabled(result));
        consumerExecutionContext.setWithLastSep(getWithLastSep(result));
        consumerExecutionContext.setQuoteEncloseMode(getQuoteEncloseMode(result));
        consumerExecutionContext.setTpsLimit(getTpsLimit(result));
        consumerExecutionContext.setUseColumns(getUseColumns(result));
        consumerExecutionContext.setEmptyStrAsNull(getEmptyAsNull(result));
        consumerExecutionContext.setMaxRetry(getMaxErrorCount(result));

        consumerExecutionContext.validate();
    }

    private static String getUseColumns(ConfigResult result) {
        List<String> columnNames = getColumnNames(result);
        if (columnNames == null) {
            return null;
        }
        return StringUtils.join(columnNames, ",");
    }

    private static boolean getWhereInEnabled(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_USING_IN);
    }

    private static boolean getReadAndProcessFileOnly(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_READ_FILE_ONLY);
    }

    private static String getDbName(ConfigResult result) {
        return result.getOptionValue(ARG_SHORT_DBNAME);
    }

    private static int getConsumerParallelism(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_CONSUMER)) {
            int parallelism = Integer.parseInt(result.getOptionValue(ARG_SHORT_CONSUMER));
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Consumer parallelism should be > 0");
            }
            return parallelism;
        } else {
            return ConfigConstant.DEFAULT_CONSUMER_SIZE;
        }
    }

    private static int getProducerParallelism(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_PRODUCER)) {
            int parallelism = Integer.parseInt(result.getOptionValue(ARG_SHORT_PRODUCER));
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Producer parallelism should be > 0");
            }
            return parallelism;
        } else {
            return ConfigConstant.DEFAULT_PRODUCER_SIZE;
        }
    }

    private static QuoteEncloseMode getQuoteEncloseMode(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_QUOTE_ENCLOSE_MODE)) {
            return QuoteEncloseMode.parseMode(result.getOptionValue(ARG_SHORT_QUOTE_ENCLOSE_MODE));
        } else {
            return ConfigConstant.DEFAULT_QUOTE_ENCLOSE_MODE;
        }
    }

    private static boolean getTrimRight(ConfigResult result) {
        return !result.getBooleanFlag(ARG_TRIM_RIGHT);
    }

    private static BenchmarkMode getBenchmarkMode(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_BENCHMARK)) {
            return BenchmarkMode.parseMode(result.getOptionValue(ARG_SHORT_BENCHMARK));
        } else {
            return BenchmarkMode.NONE;
        }
    }

    private static int getBenchmarkRound(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_FILE_NUM)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_FILE_NUM));
        } else {
            return 0;
        }
    }

    private static int getScale(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_SCALE)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_SCALE));
        } else {
            return 0;
        }
    }

    private static boolean getForceParallelism(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_FORCE_CONSUMER)) {
            return Boolean.parseBoolean(result.getOptionValue(ARG_SHORT_FORCE_CONSUMER));
        } else {
            return ConfigConstant.DEFAULT_FORCE_CONSUMER_PARALLELISM;
        }
    }

    /**
     * 解析数据文件路径与行号
     * 并检测文件是否存在
     */
    private static List<FileLineRecord> getDataFileRecordList(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_FROM_FILE)) {
            // 检查DDL文件后缀
            String filePathListStr = result.getOptionValue(ARG_SHORT_FROM_FILE);
            return Arrays.stream(StringUtils.split(filePathListStr, ConfigConstant.CMD_SEPARATOR))
                .filter(s -> StringUtils.isNotBlank(s) && !StringUtils.endsWith(s, ConfigConstant.DDL_FILE_SUFFIX))
                .map(s -> {
                    String[] strs = StringUtils.split(s, ConfigConstant.CMD_FILE_LINE_SEPARATOR);
                    if (strs.length == 1) {
                        String fileAbsPath = FileUtil.getFileAbsPath(strs[0]);
                        return new FileLineRecord(fileAbsPath);
                    } else if (strs.length == 2) {
                        String fileAbsPath = FileUtil.getFileAbsPath(strs[0]);
                        int startLine = Integer.parseInt(strs[1]);
                        return new FileLineRecord(fileAbsPath, startLine);
                    } else {
                        throw new IllegalArgumentException("Illegal file: " + s);
                    }
                }).collect(Collectors.toList());
        } else if (result.hasOption(ARG_SHORT_DIRECTORY)) {
            String dirPathStr = result.getOptionValue(ARG_SHORT_DIRECTORY);
            List<String> filePaths = FileUtil.getDataFilesAbsPathInDir(dirPathStr);
            return FileLineRecord.fromFilePaths(filePaths);
        }
        if (result.hasOption(ARG_SHORT_BENCHMARK)) {
            return null;
        }
        throw new IllegalStateException("cannot get data file path list");
    }

    /**
     * 解析数据DDL文件路径与行号
     * 并检测文件是否存在
     */
    private static List<FileLineRecord> getDdlFileRecordList(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_FROM_FILE)) {
            // 检查DDL文件后缀
            String filePathListStr = result.getOptionValue(ARG_SHORT_FROM_FILE);
            return Arrays.stream(StringUtils.split(filePathListStr, ConfigConstant.CMD_SEPARATOR))
                .filter(s -> StringUtils.isNotBlank(s) && StringUtils.endsWith(s, ConfigConstant.DDL_FILE_SUFFIX))
                .map(FileLineRecord::new).collect(Collectors.toList());
        } else if (result.hasOption(ARG_SHORT_DIRECTORY)) {
            String dirPathStr = result.getOptionValue(ARG_SHORT_DIRECTORY);
            List<String> filePaths = FileUtil.getDdlFilesAbsPathInDir(dirPathStr);
            return FileLineRecord.fromFilePaths(filePaths);
        }
        if (result.hasOption(ARG_SHORT_BENCHMARK)) {
            return null;
        }
        throw new IllegalStateException("cannot get ddl file path list");
    }

    private static int getTpsLimit(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_TPS_LIMIT)) {
            int tpsLimit = Integer.parseInt(result.getOptionValue(ARG_SHORT_TPS_LIMIT));
            if (tpsLimit <= 0) {
                throw new IllegalArgumentException("tps limit should be > 0");
            }
            return tpsLimit;
        } else {
            return ConfigConstant.DEFAULT_TPS_LIMIT;
        }
    }

    private static boolean getInsertIgnoreAndResumeEnabled(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_IGNORE_AND_RESUME);
    }

    private static DdlMode getDdlMode(ConfigResult result) {
        if (!result.hasOption(ARG_SHORT_WITH_DDL)) {
            return DdlMode.NO_DDL;
        }
        return DdlMode.fromString(result.getOptionValue(ARG_SHORT_WITH_DDL));
    }

    private static void setGlobalDdlConfig(ConfigResult result) {
        if (result.hasOption(ARG_DDL_RETRY_COUNT)) {
            GlobalVar.DDL_RETRY_COUNT = Integer.parseInt(result.getOptionValue(ARG_DDL_RETRY_COUNT));
        }
        if (result.hasOption(ARG_DDL_PARALLELISM)) {
            GlobalVar.DDL_PARALLELISM = Integer.parseInt(result.getOptionValue(ARG_DDL_PARALLELISM));
        }
    }

    private static int getMaxErrorCount(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_MAX_ERROR)) {
            int maxError = Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_ERROR));
            return Math.max(0, maxError);
        }
        return ConfigConstant.DEFAULT_MAX_ERROR_COUNT;
    }

    private static String getHistoryFile(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_HISTORY_FILE)) {
            return result.getOptionValue(ARG_SHORT_HISTORY_FILE);
        }
        return null;
    }

    private static String getWhereCondition(ConfigResult result) {
        return result.getOptionValue(ARG_SHORT_WHERE);
    }

    private static boolean getSqlEscapeEnabled(ConfigResult result) {
        return !result.getBooleanFlag(ARG_SHORT_NO_ESCAPE);
    }

    private static boolean getFuncEnabled(ConfigResult result) {
        return result.getBooleanFlag(ARG_SHORT_SQL_FUNC);
    }
    //endregion 写入数据库操作的设置

    //region 全局相关设置
    private static void setRingBufferSize(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_RING_BUFFER_SIZE)) {
            int size = Integer.parseInt(result.getOptionValue(ARG_SHORT_RING_BUFFER_SIZE));
            if (Integer.bitCount(size) != 1) {
                throw new IllegalArgumentException("Ring buffer size should be power of 2");
            }
            GlobalVar.DEFAULT_RING_BUFFER_SIZE = size;
        }
    }

    private static void setBatchSize(ConfigResult result) {
        if (result.hasOption(ARG_SHORT_BATCH_SIZE)) {
            GlobalVar.EMIT_BATCH_SIZE = Integer.parseInt(
                result.getOptionValue(ARG_SHORT_BATCH_SIZE));
        }
    }

    private static void setPerfMode(ConfigResult result) {
        GlobalVar.IN_PERF_MODE = result.getBooleanFlag(ARG_SHORT_PERF_MODE);
    }
    //endregion 全局相关设置

    //region 命令行参数校验与帮助
    /**
     * 保证命令有参数 argShort
     * 否则抛出异常
     */
    private static void requireArg(ConfigResult result, ConfigArgOption argShort) {
        if (!result.hasOption(argShort)) {
            throw new IllegalArgumentException("Missing required argument: " + argShort);
        }
    }

    /**
     * 有且仅有其中一个参数
     */
    private static void requireOnlyOneArg(ConfigResult result, ConfigArgOption ... argsShort) {
        boolean contains = false;
        for (ConfigArgOption arg : argsShort) {
            if (result.hasOption(arg)) {
                if (contains) {
                    throw new IllegalArgumentException("can only exists one of these arguments: " + StringUtils.join(argsShort, ", "));
                } else {
                    contains = true;
                }
            }
        }
        if (!contains) {
            throw new IllegalArgumentException("Missing one of these arguments: " + StringUtils.join(argsShort, ", "));
        }
    }

    /**
     * 打印帮助信息
     */
    public static void printHelp() {
        formatter.printHelp(ConfigConstant.APP_NAME, options, true);
    }

    private static CommandType lookup(String commandType) {
        for (CommandType type : CommandType.values()) {
            if (type.name().equalsIgnoreCase(commandType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Do not support command " + commandType);
    }

    public static boolean doHelpCmd(ConfigResult ConfigResult) {
        if (CommandUtil.isShowHelp(ConfigResult)) {
            printHelp();
            return true;
        }

        if (CommandUtil.isShowVersion(ConfigResult)) {
            System.out.printf("%s: %s%n", ConfigConstant.APP_NAME, Version.getVersion());
            return true;
        }
        return false;
    }

    private static boolean isShowHelp(ConfigResult result) {
        return result.hasOption(ARG_SHORT_HELP);
    }

    private static boolean isShowVersion(ConfigResult result) {
        return result.hasOption(ARG_SHORT_VERSION);
    }
    //endregion 命令行参数校验与帮助
}
