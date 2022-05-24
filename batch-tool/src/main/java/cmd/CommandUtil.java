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

import com.google.common.collect.Lists;
import datasource.DataSourceConfig;
import datasource.DatasourceConstant;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.BaseConfig;
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static model.config.ConfigConstant.*;

/**
 * 从命令行输入解析配置
 */
public class CommandUtil {
    private static final Logger logger = LoggerFactory.getLogger(CommandUtil.class);

    private static final HelpFormatter formatter = new HelpFormatter();
    private static final Options options = new Options();

    static {
        addConnectDbOptions(options);
        addBatchOperationOptions(options);
        // 添加帮助选项 -? --help
        options.addOption(Option.builder(ARG_SHORT_HELP)
            .longOpt("help")
            .desc("Help message.")
            .build());
        // 添加版本信息 -v --version
        options.addOption(Option.builder(ARG_SHORT_VERSION)
            .longOpt("version")
            .desc("Show version")
            .build());
    }

    /**
     * 解析程序启动参数
     */
    public static CommandLine parseStartUpCommand(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine result = null;
        try {
            // 开始解析命令行参数
            result = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            printHelp();
        }
        return result;
    }

    //region 数据源设置
    private static void validateDataSourceArgs(CommandLine result) {
        requireArg(result, ARG_SHORT_HOST);
        requireArg(result, ARG_SHORT_USERNAME);
        requireArg(result, ARG_SHORT_PASSWORD);
        requireArg(result, ARG_SHORT_DBNAME);
    }

    public static DataSourceConfig getDataSourceConfigFromCmd(CommandLine result) {
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

        if (result.hasOption(ARG_SHORT_LOAD_BALANCE)) {
            configBuilder.loadBalanceEnabled(true);
        } else {
            configBuilder.port(result.getOptionValue(ARG_SHORT_PORT))
                .loadBalanceEnabled(false);
        }
        return configBuilder.build();
    }

    private static int getMaxWait(CommandLine result) {
        if (result.hasOption(ARG_SHORT_MAX_WAIT)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_WAIT));
        } else {
            return DatasourceConstant.MAX_WAIT_TIME;
        }
    }

    private static int getMaxConnNum(CommandLine result) {
        if (result.hasOption(ARG_SHORT_MAX_CONN_NUM)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_CONN_NUM));
        } else {
            return DatasourceConstant.MAX_CONN_NUM;
        }
    }

    private static int getMinConnNum(CommandLine result) {
        if (result.hasOption(ARG_SHORT_MIN_CONN_NUM)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MIN_CONN_NUM));
        } else {
            return DatasourceConstant.MIN_CONN_NUM;
        }
    }

    private static String getConnParam(CommandLine result) {
        if (result.hasOption(ARG_SHORT_CONN_PARAM)) {
            return result.getOptionValue(ARG_SHORT_CONN_PARAM);
        }
        return null;
    }

    private static String getInitSqls(CommandLine result) {
        if (result.hasOption(ARG_SHORT_CONN_INIT_SQL)) {
            return result.getOptionValue(ARG_SHORT_CONN_INIT_SQL);
        }
        return null;
    }
    //endregion 数据源设置

    //region 批处理命令解析
    public static BaseOperateCommand getOperateCommandFromCmd(CommandLine result) {
        validateOperateArgs(result);
        BaseOperateCommand command = initCommand(result);
        afterInitCommand(command, result);
        return command;
    }

    private static void validateOperateArgs(CommandLine result) {
        requireArg(result, ARG_SHORT_OPERATION);
        requireArg(result, ARG_SHORT_SEP);
        requireArg(result, ARG_SHORT_DBNAME);
    }

    private static BaseOperateCommand initCommand(CommandLine result) {
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
        command.setTableNames(getTableNames(result));
        command.setColumnNames(getColumnNames(result));
        return command;
    }

    private static void afterInitCommand(BaseOperateCommand command, CommandLine result) {
        if (result.hasOption(ARG_SHORT_ENABLE_SHARDING)) {
            boolean shardingEnabled = parseFlag(result.getOptionValue(ARG_SHORT_ENABLE_SHARDING));
            command.setShardingEnabled(shardingEnabled);
        }
    }

    private static List<String> getTableNames(CommandLine result) {
        if (!result.hasOption(ARG_SHORT_TABLE)) {
            return null;
        }
        String tableNameStr = result.getOptionValue(ARG_SHORT_TABLE);
        return Lists.newArrayList(
            StringUtils.split(tableNameStr, ConfigConstant.CMD_SEPARATOR));
    }

    private static List<String> getColumnNames(CommandLine result) {
        if (!result.hasOption(ARG_SHORT_COLUMNS)) {
            return null;
        }
        String columnNameStr = result.getOptionValue(ARG_SHORT_COLUMNS);
        return Lists.newArrayList(
            StringUtils.split(columnNameStr, ConfigConstant.CMD_SEPARATOR));
    }

    private static BaseOperateCommand parseImportCommand(CommandLine result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM, ARG_SHORT_DIRECTORY);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        return new ImportCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }

    private static BaseOperateCommand parseDeleteCommand(CommandLine result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM, ARG_SHORT_DIRECTORY);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        consumerExecutionContext.setWhereCondition(getWhereCondition(result));
        return new DeleteCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }

    private static BaseOperateCommand parseUpdateCommand(CommandLine result) {
        requireOnlyOneArg(result, ARG_SHORT_FROM, ARG_SHORT_DIRECTORY);

        ProducerExecutionContext producerExecutionContext = new ProducerExecutionContext();
        ConsumerExecutionContext consumerExecutionContext = new ConsumerExecutionContext();
        configureCommonContext(result, producerExecutionContext, consumerExecutionContext);

        consumerExecutionContext.setWhereCondition(getWhereCondition(result));
        consumerExecutionContext.setFuncSqlForUpdateEnabled(getFuncEnabled(result));
        return new UpdateCommand(getDbName(result), producerExecutionContext, consumerExecutionContext);
    }
    //endregion 批处理命令解析

    //region 读写文件相关配置
    private static String getSep(CommandLine result) {
        return result.getOptionValue(ARG_SHORT_SEP);
    }

    private static Charset getCharset(CommandLine result) {
        if (result.hasOption(ARG_SHORT_CHARSET)) {
            String charset = result.getOptionValue(ARG_SHORT_CHARSET);
            return Charset.forName(charset);
        } else {
            return ConfigConstant.DEFAULT_CHARSET;
        }
    }

    private static boolean getWithHeader(CommandLine result) {
        return result.hasOption(ARG_SHORT_WITH_HEADER);
    }

    private static CompressMode getCompressMode(CommandLine result) {
        if (result.hasOption(ARG_SHORT_COMPRESS)) {
            return CompressMode.fromString(result.getOptionValue(ARG_SHORT_COMPRESS));
        } else {
            return ConfigConstant.DEFAULT_COMPRESS_MODE;
        }
    }

    private static EncryptionConfig getEncryptionConfig(CommandLine result) {
        if (result.hasOption(ARG_SHORT_ENCRYPTION)) {
            String encryptionMode = result.getOptionValue(ARG_SHORT_ENCRYPTION);
            String key = result.getOptionValue(ARG_SHORT_KEY);
            return EncryptionConfig.parse(encryptionMode, key);
        } else {
            return DEFAULT_ENCRYPTION_CONFIG;
        }
    }

    private static int getReadBlockSizeInMb(CommandLine result) {
        if (result.hasOption(ARG_SHORT_READ_BLOCK_SIZE)) {
            return Integer.parseInt(
                result.getOptionValue(ARG_SHORT_READ_BLOCK_SIZE));
        } else {
            return ConfigConstant.DEFAULT_READ_BLOCK_SIZE_IN_MB;
        }
    }

    private static boolean getWithLastSep(CommandLine result) {
        return result.hasOption(ARG_SHORT_WITH_LAST_SEP);
    }

    /**
     * TODO 文件格式、压缩格式、加密模式三者的设置冲突解决
     */
    private static FileFormat getFileFormat(CommandLine result) {
        if (result.hasOption(ARG_SHORT_FILE_FORMAT)) {
            String fileFormat = result.getOptionValue(ARG_SHORT_FILE_FORMAT);
            return FileFormat.fromString(fileFormat);
        } else {
            return DEFAULT_FILE_FORMAT;
        }
    }
    //endregion 读写文件相关配置

    //region 导出相关设置
    private static ExportCommand parseExportCommand(CommandLine result) {
        List<String> tableNames = getTableNames(result);
        ExportConfig exportConfig = new ExportConfig();
        exportConfig.setCharset(getCharset(result));
        exportConfig.setWithHeader(getWithHeader(result));
        exportConfig.setSeparator(getSep(result));
        exportConfig.setWhereCondition(getWhereCondition(result));
        exportConfig.setDdlMode(getDdlMode(result));
        exportConfig.setEncryptionConfig(getEncryptionConfig(result));
        exportConfig.setFileFormat(getFileFormat(result));
        exportConfig.setCompressMode(getCompressMode(result));
        exportConfig.setParallelism(getProducerParallelism(result));
        exportConfig.setQuoteEncloseMode(getQuoteEncloseMode(result));
        setFilenamePrefix(result, exportConfig);
        setFileNum(result, exportConfig);
        setFileLine(result, exportConfig);
        setOrderBy(result, exportConfig);
        exportConfig.validate();
        return new ExportCommand(getDbName(result), tableNames, exportConfig);
    }

    private static void setFilenamePrefix(CommandLine result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_PREFIX)) {
            exportConfig.setFilenamePrefix(result.getOptionValue(ARG_SHORT_PREFIX));
        } else {
            exportConfig.setFilenamePrefix("");
        }
    }

    private static void setOrderBy(CommandLine result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_ORDER)) {
            if (!result.hasOption(ARG_SHORT_ORDER_COLUMN)) {
                throw new IllegalArgumentException("Order column name cannot be empty");
            }
            if (result.hasOption(ARG_SHORT_LOCAL_MERGE)) {
                exportConfig.setLocalMerge(true);
            }
            exportConfig
                .setAscending(!ConfigConstant.ORDER_BY_TYPE_DESC.equals(result.getOptionValue(ARG_SHORT_ORDER)));
            List<String> columnNameList = Arrays.asList(StringUtils.split(result.getOptionValue(ARG_SHORT_ORDER_COLUMN),
                CMD_SEPARATOR));
            exportConfig.setOrderByColumnNameList(columnNameList);
            exportConfig.setParallelMerge(getParaMerge(result));
        }
    }

    private static void setFileLine(CommandLine result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_LINE)) {
            if (exportConfig.getExportWay() != ExportConfig.ExportWay.DEFAULT) {
                // 只能指定一个导出方式
                throw new IllegalArgumentException("Export way should be unique");
            }
            exportConfig.setExportWay(ExportConfig.ExportWay.MAX_LINE_NUM_IN_SINGLE_FILE);
            exportConfig.setMaxLine(Integer.parseInt(result.getOptionValue(ARG_SHORT_LINE)));
        }
    }

    private static void setFileNum(CommandLine result, ExportConfig exportConfig) {
        if (result.hasOption(ARG_SHORT_FILE_NUM)) {
            exportConfig.setExportWay(ExportConfig.ExportWay.FIXED_FILE_NUM);
            exportConfig.setFixedFileNum(Integer.parseInt(result.getOptionValue(ARG_SHORT_FILE_NUM)));
            if (exportConfig.getLimitNum() <= 0) {
                throw new IllegalArgumentException("File num should be a positive integer");
            }
        }
    }

    private static boolean getParaMerge(CommandLine result) {
        return result.hasOption(ARG_SHORT_PARALLEL_MERGE);
    }
    //endregion 导出相关设置

    //region 写入数据库操作的设置
    /**
     * 主要针对插入/更新/删除
     * 配置公共的上下文执行环境
     */
    private static void configureCommonContext(CommandLine result,
                                               ProducerExecutionContext producerExecutionContext,
                                               ConsumerExecutionContext consumerExecutionContext) {
        configureGlobalVar(result);
        configureProducerContext(result, producerExecutionContext);
        configureConsumerContext(result, consumerExecutionContext);
    }

    /**
     * 设置全局可调参数
     */
    private static void configureGlobalVar(CommandLine result) {
        setBatchSize(result);
        setRingBufferSize(result);
        setPerfMode(result);
    }

    /**
     * 配置生产者
     */
    private static void configureProducerContext(CommandLine result,
                                                 ProducerExecutionContext producerExecutionContext) {
        producerExecutionContext.setCharset(getCharset(result));
        producerExecutionContext.setSeparator(getSep(result));
        producerExecutionContext.setFileLineRecordList(getFileRecordList(result));
        producerExecutionContext.setParallelism(getProducerParallelism(result));
        producerExecutionContext.setReadBlockSizeInMb(getReadBlockSizeInMb(result));
        producerExecutionContext.setWithHeader(getWithHeader(result));
        producerExecutionContext.setDdlMode(getDdlMode(result));
        producerExecutionContext.setCompressMode(getCompressMode(result));
        producerExecutionContext.setEncryptionConfig(getEncryptionConfig(result));
        producerExecutionContext.setFileFormat(getFileFormat(result));
        producerExecutionContext.setMaxErrorCount(getMaxErrorCount(result));
        producerExecutionContext.setHistoryFileAndParse(getHistoryFile(result));
        producerExecutionContext.setQuoteEncloseMode(getQuoteEncloseMode(result));

        producerExecutionContext.validate();
    }


    /**
     * 配置消费者
     */
    private static void configureConsumerContext(CommandLine result,
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
        consumerExecutionContext.setTpsLimit(getTpsLimit(result));
        consumerExecutionContext.setUseColumns(getUseColumns(result));

        consumerExecutionContext.validate();
    }

    private static String getUseColumns(CommandLine result) {
        List<String> columnNames = getColumnNames(result);
        if (columnNames == null) {
            return null;
        }
        return StringUtils.join(columnNames, ",");
    }

    private static boolean getWhereInEnabled(CommandLine result) {
        return result.hasOption(ARG_SHORT_USING_IN);
    }

    private static boolean getReadAndProcessFileOnly(CommandLine result) {
        return result.hasOption(ARG_SHORT_READ_FILE_ONLY);
    }

    private static String getDbName(CommandLine result) {
        return result.getOptionValue(ARG_SHORT_DBNAME);
    }

    private static int getConsumerParallelism(CommandLine result) {
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

    private static int getProducerParallelism(CommandLine result) {
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

    private static QuoteEncloseMode getQuoteEncloseMode(CommandLine result) {
        if (result.hasOption(ARG_SHORT_QUOTE_ENCLOSE_MODE)) {
            return QuoteEncloseMode.parseMode(result.getOptionValue(ARG_SHORT_QUOTE_ENCLOSE_MODE));
        } else {
            return DEFAULT_QUOTE_ENCLOSE_MODE;
        }
    }

    private static boolean getForceParallelism(CommandLine result) {
        if (result.hasOption(ARG_SHORT_FORCE_CONSUMER)) {
            return Boolean.parseBoolean(result.getOptionValue(ARG_SHORT_FORCE_CONSUMER));
        } else {
            return ConfigConstant.DEFAULT_FORCE_CONSUMER_PARALLELISM;
        }
    }

    /**
     * 解析文件路径与行号
     * 并检测文件是否存在
     */
    private static List<FileLineRecord> getFileRecordList(CommandLine result) {
        if (result.hasOption(ARG_SHORT_FROM)) {
            String filePathListStr = result.getOptionValue(ARG_SHORT_FROM);
            return Arrays.stream(StringUtils.split(filePathListStr, CMD_SEPARATOR))
                .filter(StringUtils::isNotBlank)
                .map(s -> {
                    String[] strs = StringUtils.split(s, CMD_FILE_LINE_SEPARATOR);
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
            List<String> filePaths = FileUtil.getFilesAbsPathInDir(dirPathStr);
            return FileLineRecord.fromFilePaths(filePaths);
        }
        throw new IllegalStateException("cannot get file path list");
    }

    private static int getTpsLimit(CommandLine result) {
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

    private static boolean getInsertIgnoreAndResumeEnabled(CommandLine result) {
        return result.hasOption(ARG_SHORT_IGNORE_AND_RESUME);
    }

    private static DdlMode getDdlMode(CommandLine result) {
        if (!result.hasOption(ARG_SHORT_WITH_DDL)) {
            return DdlMode.NO_DDL;
        }
        return DdlMode.fromString(result.getOptionValue(ARG_SHORT_WITH_DDL));
    }

    private static int getMaxErrorCount(CommandLine result) {
        if (result.hasOption(ARG_SHORT_MAX_ERROR)) {
            return Integer.parseInt(result.getOptionValue(ARG_SHORT_MAX_ERROR));
        } else {
            return DEFAULT_MAX_ERROR_COUNT;
        }
    }

    private static String getHistoryFile(CommandLine result) {
        if (result.hasOption(ARG_SHORT_HISTORY_FILE)) {
            return result.getOptionValue(ARG_SHORT_HISTORY_FILE);
        } else {
            return null;
        }
    }

    private static String getWhereCondition(CommandLine result) {
        return result.getOptionValue(ARG_SHORT_WHERE);
    }

    private static boolean getSqlEscapeEnabled(CommandLine result) {
        return !result.hasOption(ARG_SHORT_NO_ESCAPE);
    }

    private static boolean getFuncEnabled(CommandLine result) {
        return result.hasOption(ARG_SHORT_SQL_FUNC);
    }
    //endregion 写入数据库操作的设置

    //region 全局相关设置
    private static void setRingBufferSize(CommandLine result) {
        if (result.hasOption(ARG_SHORT_RING_BUFFER_SIZE)) {
            int size = Integer.parseInt(result.getOptionValue(ARG_SHORT_RING_BUFFER_SIZE));
            if (Integer.bitCount(size) != 1) {
                throw new IllegalArgumentException("Ring buffer size should be power of 2");
            }
            GlobalVar.DEFAULT_RING_BUFFER_SIZE = size;
        }
    }

    private static void setBatchSize(CommandLine result) {
        if (result.hasOption(ARG_SHORT_BATCH_SIZE)) {
            GlobalVar.EMIT_BATCH_SIZE = Integer.parseInt(
                result.getOptionValue(ARG_SHORT_BATCH_SIZE));
        }
    }

    private static void setPerfMode(CommandLine result) {
        GlobalVar.IN_PERF_MODE = result.hasOption(ARG_SHORT_PERF_MODE);
    }
    //endregion 全局相关设置

    //region 命令行参数校验与帮助
    /**
     * 保证命令有参数 argShort
     * 否则抛出异常
     */
    private static void requireArg(CommandLine result, String argShort) {
        if (!result.hasOption(argShort)) {
            throw new IllegalArgumentException("Missing required argument: " + argShort);
        }
    }

    /**
     * 有且仅有其中一个参数
     */
    private static void requireOnlyOneArg(CommandLine result, String ... argsShort) {
        boolean contains = false;
        for (String arg : argsShort) {
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

    private static void addBatchOperationOptions(Options options) {
        // 添加批量操作选项 -o --operation
        options.addOption(Option.builder(ARG_SHORT_OPERATION)
            .longOpt("operation")
            .hasArg()
            .argName("operation")
            .desc("Batch operation type: export / import / delete / update.")
            .build());
        // 添加待操作数据表选项 -t --table
        options.addOption(Option.builder(ARG_SHORT_TABLE)
            .longOpt("table")
            .hasArg()
            .argName("table")
            .desc("Target table.")
            .build());
        // 添加分隔符选项 -s --sep
        options.addOption(Option.builder(ARG_SHORT_SEP)
            .longOpt("sep")
            .hasArg()
            .argName("sep")
            .desc("Separator between fields (delimiter).")
            .build());
        // 添加文件名前缀选项 -pre --prefix
        options.addOption(Option.builder(ARG_SHORT_PREFIX)
            .longOpt("prefix")
            .hasArg()
            .argName("prefix")
            .desc("Export file name prefix.")
            .build());
        // 添加文件源选项 -f --from
        options.addOption(Option.builder(ARG_SHORT_FROM)
            .longOpt("from")
            .hasArg()
            .argName("from")
            .desc("Source file(s), separated by ; .")
            .build());
        // 添加导出文件行数限制选项 -L --line
        options.addOption(Option.builder(ARG_SHORT_LINE)
            .longOpt("line")
            .hasArg()
            .argName("line")
            .desc("Max line limit of exported files.")
            .build());
        // 添加导出文件个数限制选项 -F --filenum
        options.addOption(Option.builder(ARG_SHORT_FILE_NUM)
            .longOpt("filenum")
            .hasArg()
            .argName("filenum")
            .desc("Fixed number of exported files.")
            .build());
        // 添加导出where条件选项 -w --where
        options.addOption(Option.builder(ARG_SHORT_WHERE)
            .longOpt("where")
            .hasArg()
            .argName("where")
            .desc("Where condition: col1>99 AND col2<100 ...")
            .build());
        // 添加insert ignore开关选项 -i --ignore
        options.addOption(Option.builder(ARG_SHORT_IGNORE_AND_RESUME)
            .longOpt("ignoreandresume")
            .argName("ignore")
            .desc("Flag of insert ignore and resume breakpoint.")
            .build());
        // 添加historyfile文件名配置 -H --historyfile
        options.addOption(Option.builder(ARG_SHORT_HISTORY_FILE)
            .longOpt("historyFile")
            .hasArg()
            .argName("history file name")
            .desc("Configure of historyfile name.")
            .build());
        // 添加限流配置
        options.addOption(Option.builder(ARG_SHORT_TPS_LIMIT)
            .longOpt("tpsLimit")
            .hasArg()
            .argName("tps limit")
            .desc("Configure of tps limit, default -1: no limit.")
            .build());
        // 添加生产者线程数选项
        options.addOption(Option.builder(ARG_SHORT_PRODUCER)
            .longOpt("producer")
            .hasArg()
            .argName("producer count")
            .desc("Configure number of producer threads (export / import).")
            .build());
        // 添加消费者者线程数选项
        options.addOption(Option.builder(ARG_SHORT_CONSUMER)
            .longOpt("consumer")
            .hasArg()
            .argName("consumer count")
            .desc("Configure number of consumer threads.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_FORCE_CONSUMER)
            .longOpt("force consumer")
            .hasArg()
            .argName("use force consumer")
            .desc("Configure if allow force consumer parallelism.")
            .build());
        // 添加只读取文件并处理选项
        options.addOption(Option.builder(ARG_SHORT_READ_FILE_ONLY)
            .longOpt("rfonly")
            .desc("Only read and process file, no sql execution.")
            .build());
        // 添加只读取文件并处理选项
        options.addOption(Option.builder(ARG_SHORT_USING_IN)
            .longOpt("wherein")
            .desc("Using where ... in (...)")
            .build());
        // 添加每行最后以分隔符结尾开关选项
        options.addOption(Option.builder(ARG_SHORT_WITH_LAST_SEP)
            .longOpt("withLastSep")
            .desc("Whether line ends with separator.")
            .build());
        // 添加并行归并选项
        options.addOption(Option.builder(ARG_SHORT_PARALLEL_MERGE)
            .longOpt("paraMerge")
            .desc("Using parallel merge when doing order by export.")
            .build());
        // 添加header是否为字段名选项
        options.addOption(Option.builder(ARG_SHORT_WITH_HEADER)
            .longOpt("header")
            .desc("Whether the header line is column names.")
            .build());
        // 添加引号转义模式
        options.addOption(Option.builder(ARG_SHORT_QUOTE_ENCLOSE_MODE)
            .longOpt("quoteMode")
            .hasArg()
            .argName("auto/force/none")
            .desc("The mode of how field values are enclosed by double-quotes when exporting table."
                + " Default value is auto.")
            .build());
        // 添加导出/导入DDL建表语句模式
        options.addOption(Option.builder(ARG_SHORT_WITH_DDL)
            .longOpt("DDL")
            .hasArg()
            .desc("Export or import with table definition DDL mode: NONE / ONLY / WITH")
            .build());
        // 添加导出/导入使用的压缩模式
        options.addOption(Option.builder(ARG_SHORT_COMPRESS)
            .longOpt("compress")
            .hasArg()
            .desc("Export or import compressed file: NONE / GZIP")
            .build());
        // 加解密算法
        options.addOption(Option.builder(ARG_SHORT_ENCRYPTION)
            .longOpt("encrypt")
            .hasArg()
            .desc("Export or import with encrypted file: NONE / AES-CBC")
            .build());
        // 对称加解密密钥
        options.addOption(Option.builder(ARG_SHORT_KEY)
            .longOpt("key")
            .hasArg()
            .desc("Encryption key (string).")
            .build());
        // 文件格式
        options.addOption(Option.builder(ARG_SHORT_FILE_FORMAT)
            .longOpt("fileformat")
            .hasArg()
            .desc("File format: NONE / TXT / CSV")
            .build());
        // 最大错误阈值
        options.addOption(Option.builder(ARG_SHORT_MAX_ERROR)
            .longOpt("max-error")
            .hasArg()
            .desc("Max error count threshold.")
            .build());
        // 性能模式
        options.addOption(Option.builder(ARG_SHORT_PERF_MODE)
            .longOpt("perf")
            .desc("perf mode")
            .build());
    }

    private static void addConnectDbOptions(Options options) {
        // 添加主机选项 -h --host
        options.addOption(Option.builder(ARG_SHORT_HOST)
            .longOpt("host")
            .hasArg()
            .argName("host")
            .desc("Connect to host.")
            .build());
        // 添加用户名选项 -u --user
        options.addOption(Option.builder(ARG_SHORT_USERNAME)
            .longOpt("user")
            .hasArg()
            .argName("user")
            .desc("User for login.")
            .build());
        // 添加密码选项 -p --password
        options.addOption(Option.builder(ARG_SHORT_PASSWORD)
            .longOpt("password")
            .hasArg()
            .argName("password")
            .desc("Password to use when connecting to server.")
            .build());
        // 添加端口选项 -P --port
        options.addOption(Option.builder(ARG_SHORT_PORT)
            .longOpt("port")
            .hasArg()
            .argName("port")
            .desc("Port number to use for connection.")
            .build());
        // 添加数据库选项 -D --database
        options.addOption(Option.builder(ARG_SHORT_DBNAME)
            .longOpt("database")
            .hasArg()
            .argName("database")
            .desc("Database to use.")
            .build());
        // 添加负载均衡开关选项 -lb --loadbalance
        options.addOption(Option.builder(ARG_SHORT_LOAD_BALANCE)
            .longOpt("loadbalance")
            .argName("loadbalance")
            .desc("If using load balance.")
            .build());
        // 添加连接参数选项 -param --connParam
        options.addOption(Option.builder(ARG_SHORT_CONN_PARAM)
            .longOpt("connParam")
            .hasArg()
            .argName("params")
            .desc("Connection params")
            .build());
        // 添加导出时选项 -O --orderby
        options.addOption(Option.builder(ARG_SHORT_ORDER)
            .longOpt("orderby")
            .hasArg()
            .argName("order by type")
            .desc("asc or desc.")
            .build());
        // 添加导入时选择文件夹选项 -dir --dir
        options.addOption(Option.builder(ARG_SHORT_DIRECTORY)
            .longOpt("dir")
            .hasArg()
            .argName("directory")
            .desc("Directory path including files to import.")
            .build());
        // 添加指定字符集选项 -cs --charset
        options.addOption(Option.builder(ARG_SHORT_CHARSET)
            .longOpt("charset")
            .hasArg()
            .argName("charset")
            .desc("Define charset of files.")
            .build());
        // 添加显示开启分库分表操作模式选项 -sharding --sharding
        options.addOption(Option.builder(ARG_SHORT_ENABLE_SHARDING)
            .longOpt("sharding")
            .hasArg()
            .argName("on | off")
            .desc("Enable sharding mode [on | off].")
            .build());
        // 添加排序列选项 -OC --orderCol
        options.addOption(Option.builder(ARG_SHORT_ORDER_COLUMN)
            .longOpt("orderCol")
            .hasArg()
            .argName("ordered column")
            .desc("col1;col2;col3")
            .build());
        // 添加指定列与顺序选项 -col --columns
        options.addOption(Option.builder(ARG_SHORT_COLUMNS)
            .longOpt("columns")
            .hasArg()
            .argName("export columns")
            .desc("col1;col2;col3")
            .build());
        // 添加在本地做归并选项 -local --local
        options.addOption(Option.builder(ARG_SHORT_LOCAL_MERGE)
            .longOpt("localmerge")
            .desc("Use local merge sort.")
            .build());
        // 添加使用sql函数更新选项 -func
        options.addOption(Option.builder(ARG_SHORT_SQL_FUNC)
            .longOpt("sqlfunc")
            .desc("Use sql function to update.")
            .build());
        // 不开启转义 -noesc
        options.addOption(Option.builder(ARG_SHORT_NO_ESCAPE)
            .longOpt("noescape")
            .desc("Don't escape values.")
            .build());
        // 连接池配置选项
        options.addOption(Option.builder(ARG_SHORT_MAX_CONN_NUM)
            .longOpt("maxConnection")
            .hasArg()
            .desc("Max connection number limit.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_MIN_CONN_NUM)
            .longOpt("minConnection")
            .hasArg()
            .desc("Mim connection number limit.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_MAX_WAIT)
            .longOpt("connMaxWait")
            .hasArg()
            .desc("Max wait time(ms) when getting a connection.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_CONN_INIT_SQL)
            .longOpt("initSqls")
            .hasArg()
            .desc("Connection init sqls.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_BATCH_SIZE)
            .longOpt("batchSize")
            .hasArg()
            .desc("Batch size of emitted tuples.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_READ_BLOCK_SIZE)
            .longOpt("readSize")
            .hasArg()
            .desc("Read block size in MB.")
            .build());
        options.addOption(Option.builder(ARG_SHORT_RING_BUFFER_SIZE)
            .longOpt("ringBufferSize")
            .hasArg()
            .desc("Ring buffer size.")
            .build());
    }

    /**
     * 打印帮助信息
     */
    public static void printHelp() {
        formatter.printHelp(APP_NAME, options, true);
    }

    private static CommandType lookup(String commandType) {
        for (CommandType type : CommandType.values()) {
            if (type.name().equalsIgnoreCase(commandType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Do not support command " + commandType);
    }

    /**
     * 解析 ON | OFF | TRUE | FALSE 字符串
     */
    private static boolean parseFlag(String flag) {
        if (StringUtils.isEmpty(flag)) {
            return false;
        }
        flag = StringUtils.strip(flag);
        if (flag.equalsIgnoreCase("ON") || flag.equalsIgnoreCase("TRUE")) {
            return true;
        }
        if (flag.equalsIgnoreCase("OFF") || flag.equalsIgnoreCase("FALSE")) {
            return false;
        }
        throw new IllegalArgumentException("Illegal flag string: " + flag + ". Should be ON or OFF");
    }

    public static boolean doHelpCmd(CommandLine commandLine) {
        if (CommandUtil.isShowHelp(commandLine)) {
            printHelp();
            return true;
        }

        if (CommandUtil.isShowVersion(commandLine)) {
            System.out.printf("%s: %s%n", ConfigConstant.APP_NAME, Version.getVersion());
            return true;
        }
        return false;
    }

    private static boolean isShowHelp(CommandLine result) {
        return result.hasOption(ARG_SHORT_HELP);
    }

    private static boolean isShowVersion(CommandLine result) {
        return result.hasOption(ARG_SHORT_VERSION);
    }
    //endregion 命令行参数校验与帮助
}
