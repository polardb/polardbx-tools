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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CountStat;
import util.SyncUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.delete.DeleteConsumer;
import worker.delete.DeleteInConsumer;
import worker.delete.ShardedDeleteInConsumer;
import worker.tpch.consumer.TpchDeleteConsumer;
import worker.tpch.consumer.TpchInsert2Consumer;
import worker.tpch.model.BatchDeleteSqlEvent;
import worker.tpch.model.BatchInsertSql2Event;
import worker.tpch.pruducer.TpchUDeleteProducer;
import worker.tpch.pruducer.TpchUInsertProducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DeleteExecutor.class);

    public DeleteExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void handleSingleTableInner(String tableName) {
        if (command.isShardingEnabled()) {
            doShardingDelete(tableName);
        } else {
            doDefaultDelete(tableName);
        }
    }

    @Override
    public void execute() {
        if (producerExecutionContext.getBenchmarkMode() != BenchmarkMode.NONE) {
            handleBenchmark();
            return;
        }

        configurePkList();
        for (String tableName : tableNames) {
            logger.info("开始删除表数据：{}", tableName);
            try {
                handleSingleTable(tableName);
                logger.info("删除 {} 数据完成，删除计数：{}", tableName, CountStat.getDbRowCount());
            } catch (Exception e) {
                logger.error("删除 {} 表数据出现异常：{}", tableName, e.getMessage());
            }
        }
    }

    private void handleBenchmark() {
        switch (producerExecutionContext.getBenchmarkMode()) {
        case TPCH:
            handleTpchRollback();
            break;
        default:
            throw new UnsupportedOperationException("Not support " + producerExecutionContext.getBenchmarkMode());
        }
    }

    /**
     * Rollback TPC-H already updated data
     * 1. delete inserted rows
     * 2. insert deleted rows
     */
    private void handleTpchRollback() {
        checkTpchUpdateTablesExist();
        final int totalRound = producerExecutionContext.getBenchmarkRound();
        if (totalRound <= 0) {
            throw new IllegalArgumentException("Use `-F` to set TPC-H rollback round");
        }

        // TPC-H update parallelism has a default limit
        int curParallelism = consumerExecutionContext.getParallelism();
        consumerExecutionContext.setParallelism(Math.min(4, curParallelism));

        for (int curRound = 1; curRound <= totalRound; curRound++) {
            logger.info("Starting TPC-H rollback round-{}, total round: {}", curRound, totalRound);
            long startTime = System.currentTimeMillis();
            doTpchDeleteForRollback(curRound);
            if (hasFatalException()) {
                logger.warn("Terminating TPC-H rollback due to fatal exception...");
                break;
            }
            doTpchInsertForRollback(curRound);
            if (hasFatalException()) {
                logger.warn("Terminating TPC-H rollback due to fatal exception...");
                break;
            }
            long endTime = System.currentTimeMillis();
            logger.info("TPC-H rollback round-{} ended, elapsed time: {}s", curRound, (endTime - startTime) / 1000);
        }
    }

    /**
     * 通过删除的方式回滚掉之前插入的数据
     */
    private void doTpchDeleteForRollback(int curRound) {
        final int producerParallelism = 1;
        AtomicInteger emittedDataCounter = SyncUtil.newRemainDataCounter();

        ThreadPoolExecutor producerThreadPool =
            MyThreadPool.createExecutorExact(TpchUDeleteProducer.class.getSimpleName(),
                producerParallelism);
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);

        int consumerParallelism = getConsumerNum(consumerExecutionContext);
        consumerExecutionContext.setParallelism(consumerParallelism);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        ThreadPoolExecutor consumerThreadPool =
            MyThreadPool.createExecutorExact(TpchDeleteConsumer.class.getSimpleName(),
                consumerParallelism);
        EventFactory<BatchDeleteSqlEvent> factory = BatchDeleteSqlEvent::new;
        RingBuffer<BatchDeleteSqlEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);
        TpchUDeleteProducer tpchProducer =
            new TpchUDeleteProducer(producerExecutionContext, ringBuffer, curRound, true);
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

    /**
     * 通过插入的方式回滚掉之前删除的数据
     */
    private void doTpchInsertForRollback(int curRound) {
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
        TpchUInsertProducer tpchProducer =
            new TpchUInsertProducer(producerExecutionContext, ringBuffer, curRound, true);
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

    private void doDefaultDelete(String tableName) {
        if (consumerExecutionContext.isWhereInEnabled()) {
            // 使用delete ... in (...)
            configureFieldMetaInfo();
            configureCommonContextAndRun(DeleteInConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName, useBlockReader());
        } else {
            configurePkList();
            configureCommonContextAndRun(DeleteConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName, useBlockReader());
        }
    }

    private void doShardingDelete(String tableName) {
        configureFieldMetaInfo();
        configureTopology();
        configurePartitionKey();
        configureCommonContextAndRun(ShardedDeleteInConsumer.class,
            producerExecutionContext, consumerExecutionContext, tableName, useBlockReader());
    }
}
