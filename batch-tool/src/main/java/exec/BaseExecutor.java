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
import model.config.GlobalVar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.common.BaseWorkHandler;
import worker.common.BatchLineEvent;
import worker.common.ReadFileProducer;
import worker.common.ReadFileWithBlockProducer;
import worker.common.ReadFileWithLineProducer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BaseExecutor.class);

    private final DataSourceConfig dataSourceConfig;
    protected final DataSource dataSource;

    public BaseExecutor(DataSourceConfig dataSourceConfig,
                        DataSource dataSource,
                        BaseOperateCommand baseCommand) {
        this.dataSourceConfig = dataSourceConfig;
        this.dataSource = dataSource;
        setCommand(baseCommand);
    }

    public void preCheck() {

    }

    protected void checkTableExists(String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            if (!DbUtil.checkTableExists(connection, tableName)) {
                throw new RuntimeException(String.format("Table [%s] does not exist", tableName));
            }
        } catch (SQLException | DatabaseException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    protected abstract void setCommand(BaseOperateCommand baseCommand);

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

    protected void configureCommonContextAndRun(Class<? extends BaseWorkHandler> clazz,
                                                ProducerExecutionContext producerExecutionContext,
                                                ConsumerExecutionContext consumerExecutionContext) {
        configureCommonContextAndRun(clazz, producerExecutionContext, consumerExecutionContext, true);
    }

    /**
     * 对生产者、消费者的上下文进行通用配置
     * 并开始执行任务 等待结束
     */
    protected void configureCommonContextAndRun(Class<? extends BaseWorkHandler> clazz,
                                                ProducerExecutionContext producerExecutionContext,
                                                ConsumerExecutionContext consumerExecutionContext,
                                                boolean usingBlockReader) {
        ThreadPoolExecutor producerThreadPool = MyThreadPool.createExecutorWithEnsure(clazz.getName() + "-producer",
            producerExecutionContext.getParallelism());
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        CountDownLatch countDownLatch = new CountDownLatch(producerExecutionContext.getParallelism());
        AtomicInteger emittedDataCounter = new AtomicInteger(0);
        List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter = new ArrayList<>();
        for (int i = 0; i < producerExecutionContext.getFilePathList().size(); i++) {
            eventCounter.add(new ConcurrentHashMap<Long, AtomicInteger>(16));
        }
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        producerExecutionContext.setCountDownLatch(countDownLatch);
        producerExecutionContext.setEventCounter(eventCounter);
        int consumerNum = 0;
        if (!consumerExecutionContext.isForceParallelism()) {
            consumerNum = Math.max(consumerExecutionContext.getParallelism(),
                ConfigConstant.CPU_NUM);
        } else {
            consumerNum = consumerExecutionContext.getParallelism();
        }

        consumerExecutionContext.setBatchTpsLimitPerConsumer((double) consumerExecutionContext.getTpsLimit()
            / (consumerNum * GlobalVar.EMIT_BATCH_SIZE));

        consumerExecutionContext.setParallelism(consumerNum);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        consumerExecutionContext.setEventCounter(eventCounter);

        consumerExecutionContext.setUsingBlock(usingBlockReader);

        // 检查上下文是否一致，确认能否使用上一次的断点继续
        producerExecutionContext.checkAndSetContextString(producerExecutionContext.toString() +
            consumerExecutionContext.toString());

        ThreadPoolExecutor consumerThreadPool = MyThreadPool.createExecutorWithEnsure(clazz.getName() + "-consumer",
            consumerNum);
        EventFactory<BatchLineEvent> factory = BatchLineEvent::new;
        RingBuffer<BatchLineEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);
        BaseWorkHandler[] consumers = new BaseWorkHandler[consumerNum];
        try {
            for (int i = 0; i < consumerNum; i++) {
                consumers[i] = clazz.newInstance();
                consumers[i].setConsumerContext(consumerExecutionContext);
                consumers[i].createTpsLimiter(consumerExecutionContext.getBatchTpsLimitPerConsumer());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
            System.exit(1);
        }
        logger.info("producer config {}", producerExecutionContext);
        logger.info("consumer config {}", consumerExecutionContext);

        WorkerPool<BatchLineEvent> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
        workerPool.start(consumerThreadPool);

        ReadFileProducer producer;
        if (usingBlockReader) {
            producer = new ReadFileWithBlockProducer(producerExecutionContext, ringBuffer);
        } else {
            producer = new ReadFileWithLineProducer(producerExecutionContext, ringBuffer);
        }

        try {
            producer.produce();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }
        // 开启断点续传和insert ignore，并且不是测试读性能模式，才开始记录断点
        if (usingBlockReader
            && consumerExecutionContext.isInsertIgnoreAndResumeEnabled()
            && !consumerExecutionContext.isReadProcessFileOnly()) {
            checkConsumeProgress((ReadFileWithBlockProducer) producer, consumers);
        }
        waitForFinish(countDownLatch, emittedDataCounter);
        workerPool.halt();
        consumerThreadPool.shutdown();
        producerThreadPool.shutdown();
    }

    /**
     * 等待生产者、消费者结束
     *
     * @param countDownLatch 生产者结束标志
     * @param emittedDataCounter 消费者结束标志
     */
    protected void waitForFinish(CountDownLatch countDownLatch, AtomicInteger emittedDataCounter) {
        try {
            // 等待生产者结束
            countDownLatch.await();
            // 等待消费者消费完成
            int remain;
            while ((remain = emittedDataCounter.get()) > 0) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        onWorkFinished();
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
