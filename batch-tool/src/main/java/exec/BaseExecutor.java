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

package exec;

import cmd.BaseOperateCommand;
import cmd.DeleteCommand;
import cmd.ExportCommand;
import cmd.ImportCommand;
import cmd.UpdateCommand;
import com.alibaba.druid.pool.DruidDataSource;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import exec.export.OrderByExportExecutor;
import exec.export.ShardingExportExecutor;
import exec.export.SingleThreadExportExecutor;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.ConfigConstant;
import model.config.ExportConfig;
import model.config.FileLineRecord;
import model.config.GlobalVar;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import util.SyncUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.common.BaseDefaultConsumer;
import worker.common.BaseWorkHandler;
import worker.common.BatchLineEvent;
import worker.common.ReadFileProducer;
import worker.common.ReadFileWithBlockProducer;
import worker.common.ReadFileWithLineProducer;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class BaseExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BaseExecutor.class);

    private final DataSourceConfig dataSourceConfig;
    protected final DataSource dataSource;
    protected final BaseOperateCommand command;

    public BaseExecutor(DataSourceConfig dataSourceConfig,
                        DataSource dataSource,
                        BaseOperateCommand baseCommand) {
        this.dataSourceConfig = dataSourceConfig;
        this.dataSource = dataSource;
        this.command = baseCommand;
    }

    public void preCheck() {

    }

    protected void checkTableExists(List<String> tableNames) {
        for (String tableName : tableNames) {
            try (Connection connection = dataSource.getConnection()) {
                if (!DbUtil.checkTableExists(connection, tableName)) {
                    throw new RuntimeException(String.format("Table [%s] does not exist", tableName));
                }
            } catch (SQLException | DatabaseException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    public abstract void execute();

    public static BaseExecutor getExecutor(BaseOperateCommand command, DataSourceConfig dataSourceConfig,
                                           DruidDataSource druid) {
        if (command instanceof ExportCommand) {
            return getExportExecutor(dataSourceConfig, druid, (ExportCommand) command);
        } else if (command instanceof ImportCommand) {
            return new ImportExecutor(dataSourceConfig, druid, command);
        } else if (command instanceof UpdateCommand) {
            return new UpdateExecutor(dataSourceConfig, druid, command);
        } else if (command instanceof DeleteCommand) {
            return new DeleteExecutor(dataSourceConfig, druid, command);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private static BaseExecutor getExportExecutor(DataSourceConfig dataSourceConfig, DruidDataSource druid,
                                                  ExportCommand command) {
        ExportConfig config = command.getExportConfig();

        if (config.getOrderByColumnNameList() != null) {
            return new OrderByExportExecutor(dataSourceConfig, druid, command);
        }
        if (command.isShardingEnabled()) {
            return new ShardingExportExecutor(dataSourceConfig, druid, command);
        }
        return new SingleThreadExportExecutor(dataSourceConfig, druid, command);
    }

    /**
     * 对生产者、消费者的上下文进行通用配置
     * 并开始执行任务 等待结束
     */
    protected void configureCommonContextAndRun(Class<? extends BaseWorkHandler> clazz,
                                                ProducerExecutionContext producerExecutionContext,
                                                ConsumerExecutionContext consumerExecutionContext,
                                                String tableName,
                                                boolean usingBlockReader) {
        List<FileLineRecord> fileLineRecordList =
            getFileRecordList(producerExecutionContext.getDataFileLineRecordList(), tableName);
        if (CollectionUtils.isEmpty(fileLineRecordList)) {
            if (command.isDbOperation()) {
                logger.warn("Skip table {} operation since no filename matches", tableName);
                return;
            }
            throw new IllegalArgumentException("No filename with suffix starts with table name: " + tableName);
        }
        String producerType;
        if (!usingBlockReader) {
            producerType = "Line";
            producerExecutionContext.setParallelism(fileLineRecordList.size());
        } else {
            producerType = "Block";
        }
        ThreadPoolExecutor producerThreadPool = MyThreadPool.createExecutorWithEnsure(producerType + "-producer",
            producerExecutionContext.getParallelism());
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        CountDownLatch countDownLatch = SyncUtil.newMainCountDownLatch(producerExecutionContext.getParallelism());
        AtomicInteger emittedDataCounter = SyncUtil.newRemainDataCounter();
        List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter = new ArrayList<>();
        for (int i = 0; i < producerExecutionContext.getDataFileLineRecordList().size(); i++) {
            eventCounter.add(new ConcurrentHashMap<>(16));
        }
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        producerExecutionContext.setCountDownLatch(countDownLatch);
        producerExecutionContext.setEventCounter(eventCounter);

        int consumerNum = getConsumerNum(consumerExecutionContext);
        consumerExecutionContext.setParallelism(consumerNum);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        consumerExecutionContext.setEventCounter(eventCounter);
        consumerExecutionContext.setUseBlock(usingBlockReader);

        consumerExecutionContext.setBatchTpsLimitPerConsumer((double) consumerExecutionContext.getTpsLimit()
            / (consumerNum * GlobalVar.EMIT_BATCH_SIZE));


        ThreadPoolExecutor consumerThreadPool = MyThreadPool.createExecutorWithEnsure(clazz.getSimpleName() + "-consumer",
            consumerNum);
        EventFactory<BatchLineEvent> factory = BatchLineEvent::new;
        RingBuffer<BatchLineEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);

        ReadFileProducer producer;
        if (usingBlockReader) {
            producer = new ReadFileWithBlockProducer(producerExecutionContext, ringBuffer, fileLineRecordList);
        } else {
            producer = new ReadFileWithLineProducer(producerExecutionContext, ringBuffer, fileLineRecordList);
        }

        consumerExecutionContext.setUseMagicSeparator(producer.useMagicSeparator());

        // 检查上下文是否一致，确认能否使用上一次的断点继续
        producerExecutionContext.checkAndSetContextString(producerExecutionContext.toString() +
            consumerExecutionContext.toString());

        BaseWorkHandler[] consumers = new BaseWorkHandler[consumerNum];
        try {
            for (int i = 0; i < consumerNum; i++) {
                BaseWorkHandler consumer = clazz.newInstance();
                consumers[i] = consumer;
                consumer.setConsumerContext(consumerExecutionContext);
                consumer.createTpsLimiter(consumerExecutionContext.getBatchTpsLimitPerConsumer());
                consumer.setTableName(tableName);
                if (consumer instanceof BaseDefaultConsumer) {
                    GlobalVar.DEBUG_INFO.addSqlStat(((BaseDefaultConsumer) consumer).getSqlStat());
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        logger.debug("producer config {}", producerExecutionContext);
        logger.debug("consumer config {}", consumerExecutionContext);

        // 开启线程工作
        WorkerPool<BatchLineEvent> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
        workerPool.start(consumerThreadPool);
        try {
            producer.produce();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        // 开启断点续传和insert ignore，并且不是测试读性能模式，才开始记录断点
        if (usingBlockReader
            && consumerExecutionContext.isInsertIgnoreAndResumeEnabled()
            && !consumerExecutionContext.isReadProcessFileOnly()) {
            checkConsumeProgress((ReadFileWithBlockProducer) producer, consumers);
        }
        waitForFinish(countDownLatch, emittedDataCounter, producerExecutionContext, consumerExecutionContext);
        workerPool.drainAndHalt();
        producerThreadPool.shutdownNow();
        consumerThreadPool.shutdownNow();
    }

    protected int getConsumerNum(ConsumerExecutionContext consumerExecutionContext) {
        if (!consumerExecutionContext.isForceParallelism()) {
            return Math.max(consumerExecutionContext.getParallelism(),
                ConfigConstant.CPU_NUM);
        } else {
            return consumerExecutionContext.getParallelism();
        }
    }

    /**
     * 获取当前导入表对应的文件路径
     */
    private List<FileLineRecord> getFileRecordList(List<FileLineRecord> allFilePathList, String tableName) {
        if (allFilePathList == null || allFilePathList.isEmpty()) {
            throw new IllegalArgumentException("File path list cannot be empty");
        }
        if (allFilePathList.size() == 1) {
            // 当只有一个文件时 无需匹配表名与文件名
            return allFilePathList;
        }
        // 匹配文件名与表名
        List<FileLineRecord> fileRecordList = allFilePathList.stream()
            .filter(fileRecord -> {
                String fileName = new File(fileRecord.getFilePath()).getName();
                if (!(fileName.length() >= tableName.length() + 2)) {
                    return false;
                }
                int i = 0;
                for (; i < tableName.length(); i++) {
                    if (tableName.charAt(i) != fileName.charAt(i)) {
                        return false;
                    }
                }
                if (fileName.charAt(i++) != '_') {
                    return false;
                }
                for (; i < fileName.length(); i++) {
                    if (fileName.charAt(i) == '.') {
                        // ignore suffix match after dot
                        break;
                    }
                    if (!Character.isDigit(fileName.charAt(i))) {
                        return false;
                    }
                }
                return true;
            })
            .collect(Collectors.toList());
        return fileRecordList;
    }

    /**
     * 等待生产者、消费者结束
     *
     * @param countDownLatch 生产者结束标志
     * @param remainDataCounter 消费者结束标志
     */
    protected void waitForFinish(CountDownLatch countDownLatch, AtomicInteger remainDataCounter) {
        try {
            SyncUtil.waitForFinish(countDownLatch, remainDataCounter);
        } catch (InterruptedException e) {
            logger.error("Interrupted when waiting for finish", e);
        }
        onWorkFinished();
    }

    protected void waitForFinish(CountDownLatch countDownLatch, AtomicInteger emittedDataCounter,
                                 ProducerExecutionContext producerContext,
                                 ConsumerExecutionContext consumerContext) {
        try {
            // 等待生产者结束
            while (!countDownLatch.await(3, TimeUnit.SECONDS)) {
                if (producerContext.getException() != null) {
                    logger.warn("Early exit because of producer exception");
                    break;
                }
                if (consumerContext.getException() != null) {
                    logger.warn("Early exit because of consumer exception");
                    producerContext.setException(consumerContext.getException());
                    break;
                }
            }
            // 等待消费者消费完成
            int remain;
            while ((remain = emittedDataCounter.get()) > 0) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted when waiting for finish", e);
        } finally {
            onWorkFinished();
        }
    }

    protected void checkConsumeProgress(ReadFileWithBlockProducer producers, BaseWorkHandler[] consumers) {

    }

    protected void onWorkFinished() {

    }

    public void close() {

    }

    protected String getSchemaName() {
        return dataSourceConfig.getDbName();
    }
}
