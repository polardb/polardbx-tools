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
import com.alibaba.druid.pool.DruidDataSource;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import datasource.DataSourceConfig;
import model.config.BenchmarkMode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SyncUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.common.BaseWorkHandler;
import worker.tpch.consumer.TpchDeleteConsumer;
import worker.tpch.consumer.TpchInsert2Consumer;
import worker.tpch.model.BatchDeleteSqlEvent;
import worker.tpch.model.BatchInsertSql2Event;
import worker.tpch.pruducer.TpchUDeleteProducer;
import worker.tpch.pruducer.TpchUInsertProducer;
import worker.update.ReplaceConsumer;
import worker.update.ShardedReplaceConsumer;
import worker.update.UpdateConsumer;
import worker.update.UpdateWithFuncConsumer;
import worker.update.UpdateWithFuncInConsumer;
import worker.util.UpdateUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(UpdateExecutor.class);

    public UpdateExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    public void execute() {
        if (producerExecutionContext.getBenchmarkMode() != BenchmarkMode.NONE) {
            handleBenchmark();
            return;
        }

        configureFieldMetaInfo();
        configurePkList();

        if (consumerExecutionContext.isFuncSqlForUpdateEnabled()) {
            // 启用函数则优先
            doUpdateWithFunc();
            logger.info("更新 {} 数据完成", tableNames);
            return;
        }
        if (!StringUtils.isEmpty(consumerExecutionContext.getWhereCondition())) {
            // 有where子句用默认方法
            doDefaultUpdate(UpdateConsumer.class);
            logger.info("更新 {} 数据完成", tableNames);
            return;
        }

        if (command.isShardingEnabled()) {
            doShardingUpdate();
        } else {
            // 禁用sharding则默认
            doDefaultUpdate(ReplaceConsumer.class);
        }
        logger.info("更新 {} 数据完成", tableNames);
    }

    private void handleBenchmark() {
        switch (producerExecutionContext.getBenchmarkMode()) {
        case TPCH:
            handleTpchUpdate();
            break;
        default:
            throw new UnsupportedOperationException("Not support " + producerExecutionContext.getBenchmarkMode());
        }
    }

    private void handleTpchUpdate() {
        checkTpchUpdateTablesExist();
        final int totalRound = producerExecutionContext.getBenchmarkRound();
        if (totalRound <= 0) {
            throw new IllegalArgumentException("Use `-F` to set TPC-H update round");
        }

        // TPC-H update parallelism has a default limit
        int curParallelism = consumerExecutionContext.getParallelism();
        consumerExecutionContext.setParallelism(Math.min(4, curParallelism));

        logger.debug("producer config {}", producerExecutionContext);
        logger.debug("consumer config {}", consumerExecutionContext);

        for (int curRound = 1; curRound <= totalRound; curRound++) {
            logger.info("Starting TPC-H update round-{}, total round: {}", curRound, totalRound);
            long startTime = System.currentTimeMillis();
            doTpchDelete(curRound);
            if (hasFatalException()) {
                logger.warn("Terminating TPC-H update due to fatal exception...");
                break;
            }
            doTpchInsert(curRound);
            if (hasFatalException()) {
                logger.warn("Terminating TPC-H update due to fatal exception...");
                break;
            }
            long endTime = System.currentTimeMillis();
            logger.info("TPC-H update round-{} ended, elapsed time: {}s", curRound, (endTime - startTime) / 1000);
        }
    }

    private void doTpchDelete(int curRound) {
        final int producerParallelism = 1;
        AtomicInteger emittedDataCounter = SyncUtil.newRemainDataCounter();

        ThreadPoolExecutor producerThreadPool = MyThreadPool.createExecutorExact(TpchUDeleteProducer.class.getSimpleName(),
            producerParallelism);
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);

        int consumerParallelism = getConsumerNum(consumerExecutionContext);
        consumerExecutionContext.setParallelism(consumerParallelism);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        ThreadPoolExecutor consumerThreadPool = MyThreadPool.createExecutorExact(TpchDeleteConsumer.class.getSimpleName(),
            consumerParallelism);
        EventFactory<BatchDeleteSqlEvent> factory = BatchDeleteSqlEvent::new;
        RingBuffer<BatchDeleteSqlEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);
        TpchUDeleteProducer tpchProducer = new TpchUDeleteProducer(producerExecutionContext, ringBuffer, curRound);
        CountDownLatch countDownLatch = SyncUtil.newMainCountDownLatch(1);
        producerExecutionContext.setCountDownLatch(countDownLatch);

        TpchDeleteConsumer[] consumers = new TpchDeleteConsumer[consumerParallelism];
        try {
            for (int i = 0; i < consumerParallelism; i++) {
                consumers[i] = new TpchDeleteConsumer(consumerExecutionContext);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // 开启线程工作
        WorkerPool<BatchDeleteSqlEvent> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
        workerPool.start(consumerThreadPool);
        try {
            tpchProducer.produce();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        waitAndShutDown(countDownLatch, emittedDataCounter, producerThreadPool, consumerThreadPool,
            workerPool);
    }

    private void doTpchInsert(int curRound) {
        final int producerParallelism = 1;
        AtomicInteger emittedDataCounter = SyncUtil.newRemainDataCounter();

        ThreadPoolExecutor producerThreadPool =
            MyThreadPool.createExecutorExact(TpchUInsertProducer.class.getSimpleName(),
                producerParallelism);
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);

        int consumerParallelism = getConsumerNum(consumerExecutionContext);
        consumerExecutionContext.setParallelism(consumerParallelism);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        ThreadPoolExecutor consumerThreadPool =
            MyThreadPool.createExecutorExact(TpchInsert2Consumer.class.getSimpleName(),
                consumerParallelism);
        EventFactory<BatchInsertSql2Event> factory = BatchInsertSql2Event::new;
        RingBuffer<BatchInsertSql2Event> ringBuffer = MyWorkerPool.createRingBuffer(factory);
        TpchUInsertProducer tpchProducer = new TpchUInsertProducer(producerExecutionContext, ringBuffer, curRound);
        CountDownLatch countDownLatch = SyncUtil.newMainCountDownLatch(1);
        producerExecutionContext.setCountDownLatch(countDownLatch);

        TpchInsert2Consumer[] consumers = new TpchInsert2Consumer[consumerParallelism];
        try {
            for (int i = 0; i < consumerParallelism; i++) {
                consumers[i] = new TpchInsert2Consumer(consumerExecutionContext);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        // 开启线程工作
        WorkerPool<BatchInsertSql2Event> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
        workerPool.start(consumerThreadPool);
        try {
            tpchProducer.produce();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        waitAndShutDown(countDownLatch, emittedDataCounter, producerThreadPool, consumerThreadPool,
            workerPool);
    }

    private void doShardingUpdate() {
        configureTopology();
        configurePartitionKey();
        for (String tableName : tableNames) {
            String toUpdateColumns =
                UpdateUtil.formatToReplaceColumns(consumerExecutionContext.getTableFieldMetaInfo(tableName));
            consumerExecutionContext.setToUpdateColumns(toUpdateColumns);
            configureCommonContextAndRun(ShardedReplaceConsumer.class, producerExecutionContext,
                consumerExecutionContext, tableName, useBlockReader());
        }
    }

    /**
     * 使用mysql函数进行字段更新
     */
    private void doUpdateWithFunc() {
        for (String tableName : tableNames) {
            String updateWithFuncSqlPattern = UpdateUtil.getUpdateWithFuncSqlPattern(tableName,
                consumerExecutionContext.getTableFieldMetaInfo(tableName).getFieldMetaInfoList(),
                consumerExecutionContext.getTablePkIndexSet(tableName));
            consumerExecutionContext.setUpdateWithFuncPattern(updateWithFuncSqlPattern);

            if (consumerExecutionContext.isWhereInEnabled()) {
                configureCommonContextAndRun(UpdateWithFuncInConsumer.class, producerExecutionContext,
                    consumerExecutionContext, tableName, useBlockReader());
            } else {
                configureCommonContextAndRun(UpdateWithFuncConsumer.class, producerExecutionContext,
                    consumerExecutionContext, tableName, useBlockReader());
            }
        }
    }

    /**
     * 无sharding
     *
     * @param clazz 决定实际的worker是使用update还是replace
     */
    private void doDefaultUpdate(Class<? extends BaseWorkHandler> clazz) {
        for (String tableName : tableNames) {
            String toUpdateColumns =
                UpdateUtil.formatToReplaceColumns(consumerExecutionContext.getTableFieldMetaInfo(tableName));
            consumerExecutionContext.setToUpdateColumns(toUpdateColumns);

            configureCommonContextAndRun(clazz, producerExecutionContext,
                consumerExecutionContext, tableName, useBlockReader());
        }
    }
}
