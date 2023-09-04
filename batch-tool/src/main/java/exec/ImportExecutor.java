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
import exception.DatabaseException;
import model.config.BenchmarkMode;
import model.config.ConfigConstant;
import model.config.DdlMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.ddl.DdlImportWorker;
import worker.insert.DirectImportWorker;
import worker.insert.ImportConsumer;
import worker.insert.ProcessOnlyImportConsumer;
import worker.insert.ShardedImportConsumer;
import worker.tpch.BatchInsertSqlEvent;
import worker.tpch.TpchConsumer;
import worker.tpch.TpchProducer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ImportExecutor.class);

    public ImportExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    public void preCheck() {
        if (producerExecutionContext.getDdlMode() == DdlMode.NO_DDL) {
            if (command.isDbOperation()) {
                try (Connection conn = dataSource.getConnection()) {
                    this.tableNames = DbUtil.getAllTablesInDb(conn, command.getDbName());
                } catch (SQLException | DatabaseException e) {
                    throw new RuntimeException(e);
                }
            } else {
                checkTableExists(command.getTableNames());
                this.tableNames = command.getTableNames();
            }
        }
        logger.info("目标导入表：{}", tableNames);
    }

    private void checkDbNotExist(String dbName) {
        if (ConfigConstant.DEFAULT_SCHEMA_NAME.equalsIgnoreCase(dbName)) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            if (DbUtil.checkDatabaseExists(conn, dbName)) {
               throw new RuntimeException(String.format("Database [%s] already exists, cannot import with ddl",
                   dbName));
            }
        } catch (SQLException | DatabaseException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void checkTableNotExist(List<String> tableNames) {
        for (String tableName : tableNames) {
            try (Connection conn = dataSource.getConnection()) {
                if (DbUtil.checkTableExists(conn, tableName)) {
                    throw new RuntimeException(String.format("Table [%s] already exists, cannot import with ddl",
                        tableNames));
                }
            } catch (SQLException | DatabaseException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void execute() {
        if (producerExecutionContext.getBenchmarkMode() != BenchmarkMode.NONE) {
            handleBenchmark(tableNames);
            return;
        }

        switch (producerExecutionContext.getDdlMode()) {
        case WITH_DDL:
            handleDDL();
            break;
        case DDL_ONLY:
            handleDDL();
            return;
        case NO_DDL:
            break;
        default:
            throw new UnsupportedOperationException("DDL mode is not supported: " +
                producerExecutionContext.getDdlMode());
        }
        configureFieldMetaInfo();

        logger.debug(producerExecutionContext.toString());
        logger.debug(consumerExecutionContext.toString());

        for (String tableName : tableNames) {
            logger.info("开始导入表：{}", tableName);
            if (producerExecutionContext.isSingleThread()
                && consumerExecutionContext.isSingleThread()) {
                // 使用按行读取insert模式
                doSingleThreadImport(tableName);
            } else {
                if (command.isShardingEnabled()) {
                    doShardingImport(tableName);
                } else {
                    doDefaultImport(tableName);
                }
            }

            if (producerExecutionContext.getException() != null) {
                logger.error("导入数据到 {} 失败：{}", tableName,
                    producerExecutionContext.getException().getMessage());
                return;
            }
            if (consumerExecutionContext.getException() != null) {
                logger.error("导入数据到 {} 失败：{}", tableName,
                    consumerExecutionContext.getException().getMessage());
                return;
            }
            logger.info("导入数据到 {} 完成", tableName);
        }
    }

    private void handleBenchmark(List<String> tableNames) {
        switch (producerExecutionContext.getBenchmarkMode()) {
        case TPCH:
            handleTpchImport(tableNames);
            break;
        default:
            throw new UnsupportedOperationException("Not support " + producerExecutionContext.getBenchmarkMode());
        }

    }

    private void handleTpchImport(List<String> tableNames) {
        int producerParallelism = producerExecutionContext.getParallelism();
        AtomicInteger emittedDataCounter = new AtomicInteger(0);

        ThreadPoolExecutor producerThreadPool = MyThreadPool.createExecutorExact(TpchProducer.class.getSimpleName(),
            producerParallelism);
        producerExecutionContext.setProducerExecutor(producerThreadPool);
        producerExecutionContext.setEmittedDataCounter(emittedDataCounter);

        int consumerParallelism = getConsumerNum(consumerExecutionContext);
        consumerExecutionContext.setParallelism(consumerParallelism);
        consumerExecutionContext.setDataSource(dataSource);
        consumerExecutionContext.setEmittedDataCounter(emittedDataCounter);
        ThreadPoolExecutor consumerThreadPool = MyThreadPool.createExecutorExact(TpchConsumer.class.getSimpleName(),
            consumerParallelism);

        EventFactory<BatchInsertSqlEvent> factory = BatchInsertSqlEvent::new;
        RingBuffer<BatchInsertSqlEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);

        TpchProducer tpchProducer = new TpchProducer(producerExecutionContext, tableNames, ringBuffer);
        CountDownLatch countDownLatch = new CountDownLatch(tpchProducer.getWorkerCount());
        producerExecutionContext.setCountDownLatch(countDownLatch);

        TpchConsumer[] consumers = new TpchConsumer[consumerParallelism];
        try {
            for (int i = 0; i < consumerParallelism; i++) {
                consumers[i] = new TpchConsumer(consumerExecutionContext);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        logger.debug("producer config {}", producerExecutionContext);
        logger.debug("consumer config {}", consumerExecutionContext);

        // 开启线程工作
        WorkerPool<BatchInsertSqlEvent> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
        workerPool.start(consumerThreadPool);
        try {
            tpchProducer.produce();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        waitForFinish(countDownLatch, emittedDataCounter, producerExecutionContext, consumerExecutionContext);
        if (producerExecutionContext.getException() != null || consumerExecutionContext.getException() != null) {
            producerThreadPool.shutdownNow();
            consumerThreadPool.shutdownNow();
            workerPool.halt();
        } else {
            workerPool.drainAndHalt();
            consumerThreadPool.shutdown();
            producerThreadPool.shutdown();
        }
    }

    /**
     * 同步导入建库建表语句
     */
    private void handleDDL() {
        DdlImportWorker ddlImportWorker;
        if (command.isDbOperation()) {
            if (producerExecutionContext.getFileLineRecordList().size() != 1) {
                throw new UnsupportedOperationException("Import database DDL only support one ddl file now!");
            }
            ddlImportWorker = new DdlImportWorker(producerExecutionContext.getFileLineRecordList()
                .get(0).getFilePath(), dataSource);
        } else {
            ddlImportWorker = new DdlImportWorker(command.getTableNames(), dataSource);
        }
        ddlImportWorker.doImportSync();
    }

    private void doSingleThreadImport(String tableName) {
        DirectImportWorker directImportWorker = new DirectImportWorker(dataSource, tableName,
            producerExecutionContext, consumerExecutionContext);
        Thread importThread = new Thread(directImportWorker);
        importThread.start();
        try {
            importThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doDefaultImport(String tableName) {
        if (consumerExecutionContext.isReadProcessFileOnly()) {
            // 测试读取文件的性能
            configureCommonContextAndRun(ProcessOnlyImportConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName, false);
        } else {
            configureCommonContextAndRun(ImportConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName,
                useBlockReader());
        }
    }

    private void doShardingImport(String tableName) {
        configurePartitionKey();
        configureTopology();

        configureCommonContextAndRun(ShardedImportConsumer.class,
            producerExecutionContext, consumerExecutionContext, tableName,
            useBlockReader());
    }

}
