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
import cmd.ExportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.CyclicAtomicInteger;
import model.config.ExportConfig;
import model.config.GlobalVar;
import model.db.FieldMetaInfo;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.MyThreadPool;
import worker.MyWorkerPool;
import worker.export.CollectFragmentWorker;
import worker.export.DirectExportWorker;
import worker.export.ExportConsumer;
import worker.export.ExportEvent;
import worker.export.ExportProducer;
import worker.export.order.DirectOrderByExportWorker;
import worker.export.order.LocalOrderByExportProducer;
import worker.export.order.OrderByExportEvent;
import worker.export.order.OrderByExportProducer;
import worker.export.order.OrderByMergeExportConsumer;
import worker.export.order.ParallelMergeExportConsumer;
import worker.export.order.ParallelOrderByExportEvent;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static model.config.ConfigConstant.APP_NAME;

public class ExportExecutor extends BaseExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ExportExecutor.class);

    private ExportCommand command;
    private ExportConfig config;

    public ExportExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (ExportCommand) baseCommand;
        this.config = command.getExportConfig();
    }

    @Override
    public void execute() {
        if (config.getOrderByColumnNameList() != null) {
            handleExportOrderBy();
            return;
        }
        if (command.isShardingEnabled()) {
            doExportWithSharding();
        } else {
            doDefaultExport();
        }
    }

    /**
     * 根据配置的算法模式
     * 按指定字段排序排序导出
     */
    private void handleExportOrderBy() {
        if (!config.isLocalMerge()) {
            handleExportWithOrderByFromDrds();
            return;
        }

        // 在本地进行多流归并排序
        if (config.isParallelMerge()) {
            handleExportWithOrderByParallelMerge();
        } else {
            doExportWithOrderByLocal();
        }
    }


    /**
     * 使用单条长连接导出数据
     */
    private void doDefaultExport() {
        throw new UnsupportedOperationException("no sharding export not supported yet");
    }

    /**
     * 处理分库分表导出命令
     * TODO 重构执行逻辑
     */
    private void doExportWithSharding() {
        List<TableTopology> topologyList;
        try {
            topologyList = DbUtil.getTopology(druid.getConnection(), command.getTableName());
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(druid.getConnection(),
                getSchemaName(), command.getTableName());
            // 分片数
            final int shardSize = topologyList.size();
            int parallelism = config.getParallelism();
            parallelism = parallelism > 0 ? parallelism : shardSize;
            Semaphore permitted = new Semaphore(parallelism, true);

            ExecutorService executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, shardSize);
            DirectExportWorker directExportWorker;
            CountDownLatch countDownLatch = new CountDownLatch(shardSize);
            switch (config.getExportWay()) {
            case MAX_LINE_NUM_IN_SINGLE_FILE:
                for (int i = 0; i < shardSize; i++) {
                    directExportWorker = new DirectExportWorker(druid,
                        topologyList.get(i), tableFieldMetaInfo,
                        config.getLimitNum(),
                        command.getFilePathPrefix() + i,
                        config.getSeparator(), config.isWithHeader(), config.getQuoteEncloseMode());
                    directExportWorker.setWhereCondition(config.getWhereCondition());
                    directExportWorker.setCountDownLatch(countDownLatch);
                    directExportWorker.setPermitted(permitted);
                    executor.submit(directExportWorker);
                }
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case DEFAULT:
                for (int i = 0; i < topologyList.size(); i++) {
                    directExportWorker = new DirectExportWorker(druid,
                        topologyList.get(i), tableFieldMetaInfo,
                        command.getFilePathPrefix() + i,
                        config.getSeparator(), config.isWithHeader(), config.getQuoteEncloseMode());
                    directExportWorker.setWhereCondition(config.getWhereCondition());
                    directExportWorker.setCountDownLatch(countDownLatch);
                    directExportWorker.setPermitted(permitted);
                    executor.submit(directExportWorker);
                }
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case FIXED_FILE_NUM:
                // 初始化缓冲区等
                EventFactory<ExportEvent> factory = ExportEvent::new;
                RingBuffer<ExportEvent> ringBuffer = MyWorkerPool.createRingBuffer(factory);
                AtomicInteger emittedDataCounter = new AtomicInteger(0);
                // 消费者数量与文件数一致 生产者数量和shard数一致
                final int consumerCount = config.getLimitNum(), producerCount = shardSize;

                ExportConsumer[] consumers = new ExportConsumer[consumerCount];
                String[] filePaths = new String[consumerCount];
                for (int i = 0; i < consumers.length; i++) {
                    filePaths[i] = command.getFilePathPrefix() + i;
                    consumers[i] = new ExportConsumer(filePaths[i], emittedDataCounter,
                        config.isWithHeader(),
                        config.getSeparator().getBytes(),
                        tableFieldMetaInfo);
                }
                WorkerPool<ExportEvent> workerPool = MyWorkerPool.createWorkerPool(ringBuffer, consumers);
                workerPool.start(executor);

                ExecutorService producerExecutor = MyThreadPool.createExecutorWithEnsure("producer", producerCount);
                // 每一个生产者最后的碎片数据处理
                if (producerCount >= consumerCount * 2) {
                    // 当生产者数量略大于消费者时 没必要轮询分配碎片
                    for (TableTopology topology : topologyList) {
                        ExportProducer producer = new ExportProducer(druid, topology,
                            tableFieldMetaInfo, ringBuffer, config.getSeparator(),
                            countDownLatch, emittedDataCounter, false, config.getQuoteEncloseMode());
                        producer.setPermitted(permitted);
                        producer.setWhereCondition(config.getWhereCondition());
                        producerExecutor.submit(producer);
                    }
                    waitForFinish(countDownLatch, emittedDataCounter);
                    workerPool.halt();
                } else {
                    Queue<ExportEvent> fragmentQueue = new ArrayBlockingQueue<>(producerCount);
                    // 将碎片放入缓冲队列
                    for (TableTopology topology : topologyList) {
                        ExportProducer producer = new ExportProducer(druid, topology,
                            tableFieldMetaInfo, ringBuffer, config.getSeparator(),
                            countDownLatch, emittedDataCounter,
                            true, config.getQuoteEncloseMode());
                        producer.setWhereCondition(config.getWhereCondition());
                        producer.setFragmentQueue(fragmentQueue);
                        producer.setPermitted(permitted);
                        producerExecutor.submit(producer);
                    }
                    CyclicAtomicInteger cyclicCounter = new CyclicAtomicInteger(consumerCount);
                    // 待消费者消费结束
                    waitForFinish(countDownLatch, emittedDataCounter);
                    workerPool.halt();
                    CountDownLatch fragmentCountLatch = new CountDownLatch(consumerCount);
                    // 再将碎片一次分配给每个文件
                    for (int i = 0; i < consumerCount; i++) {
                        CollectFragmentWorker collectFragmentWorker = new CollectFragmentWorker(
                            fragmentQueue, filePaths, cyclicCounter, fragmentCountLatch);
                        executor.submit(collectFragmentWorker);
                    }
                    try {
                        fragmentCountLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                for (ExportConsumer consumer : consumers) {
                    consumer.close();
                }
                producerExecutor.shutdown();
                break;
            default:
                throw new RuntimeException("Unsupported export exception");
            }
            executor.shutdown();
            logger.info("导出 {} 数据完成", command.getTableName());
        } catch (DatabaseException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 带有order by的导出命令处理
     * 在本地进行merge
     */
    private void doExportWithOrderByLocal() {
        List<TableTopology> topologyList;
        ExportConfig config = command.getExportConfig();
        List<FieldMetaInfo> orderByColumnInfoList;
        try {
            topologyList = DbUtil.getTopology(druid.getConnection(), command.getTableName());
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(druid.getConnection(),
                getSchemaName(), command.getTableName());
            orderByColumnInfoList = DbUtil.getFieldMetaInfoListByColNames(druid.getConnection(), getSchemaName(),
                command.getTableName(), config.getOrderByColumnNameList());
            // 分片数
            final int shardSize = topologyList.size();
            ExecutorService executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, shardSize);
            OrderByExportProducer orderByExportProducer;
            LinkedBlockingQueue[] orderedQueues = new LinkedBlockingQueue[shardSize];
            AtomicBoolean[] finishedList = new AtomicBoolean[shardSize];
            for (int i = 0; i < shardSize; i++) {
                orderedQueues[i] = new LinkedBlockingQueue<OrderByExportEvent>(GlobalVar.DEFAULT_RING_BUFFER_SIZE);
                finishedList[i] = new AtomicBoolean(false);
                orderByExportProducer = new OrderByExportProducer(druid, topologyList.get(i),
                    tableFieldMetaInfo, orderedQueues[i], i, config.getOrderByColumnNameList(),
                    finishedList[i]);
                executor.submit(orderByExportProducer);
            }
            OrderByMergeExportConsumer consumer;
            switch (config.getExportWay()) {
            case MAX_LINE_NUM_IN_SINGLE_FILE:
                consumer = new OrderByMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedQueues, finishedList, config.getLimitNum());
                break;
            case FIXED_FILE_NUM:
                // 固定文件数的情况 先拿到全部的行数
                double totalRowCount = DbUtil.getTableRowCount(druid.getConnection(), command.getTableName());
                int fileNum = config.getLimitNum();
                int singleLineLimit = (int) Math.ceil(totalRowCount / fileNum);
                // 再转为限制单文件行数的形式
                consumer = new OrderByMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedQueues, finishedList, singleLineLimit);
                break;
            case DEFAULT:
                consumer = new OrderByMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedQueues, finishedList, 0);
                break;
            default:
                throw new RuntimeException("Unsupported export exception");
            }
            try {
                consumer.consume();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executor.shutdown();
            logger.info("导出 {} 数据完成", command.getTableName());
        } catch (DatabaseException | SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    /**
     * 带有order by的导出命令处理
     * 交给drds全局排序
     */
    private void handleExportWithOrderByFromDrds() {
        ExportConfig config = command.getExportConfig();
        try {
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(druid.getConnection(),
                getSchemaName(), command.getTableName());
            int maxLine = 0;
            switch (config.getExportWay()) {
            case MAX_LINE_NUM_IN_SINGLE_FILE:
                maxLine = config.getLimitNum();
                break;
            case FIXED_FILE_NUM:
                // 固定文件数的情况 先拿到全部的行数
                double totalRowCount = DbUtil.getTableRowCount(druid.getConnection(), command.getTableName());
                int fileNum = config.getLimitNum();
                maxLine = (int) Math.ceil(totalRowCount / fileNum);
                break;
            case DEFAULT:
            default:
                break;
            }
            DirectOrderByExportWorker directOrderByExportWorker =
                new DirectOrderByExportWorker(druid, command.getFilePathPrefix(),
                    tableFieldMetaInfo,
                    command.getTableName(), config.getOrderByColumnNameList(), maxLine,
                    config.getSeparator().getBytes(),
                    config.isAscending());
            // 就单线程地写入
            directOrderByExportWorker.produceData();
            logger.info("导出 {} 数据完成", command.getTableName());
        } catch (DatabaseException | SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    /**
     * 在本地进行多线程的归并
     */
    private void handleExportWithOrderByParallelMerge() {
        List<TableTopology> topologyList;
        ExportConfig config = command.getExportConfig();
        List<FieldMetaInfo> orderByColumnInfoList;
        try {
            topologyList = DbUtil.getTopology(druid.getConnection(), command.getTableName());
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(druid.getConnection(),
                getSchemaName(), command.getTableName());
            orderByColumnInfoList = DbUtil.getFieldMetaInfoListByColNames(druid.getConnection(), getSchemaName(),
                command.getTableName(), config.getOrderByColumnNameList());
            // 分片数
            final int shardSize = topologyList.size();
            ExecutorService executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, shardSize);
            LocalOrderByExportProducer orderByExportProducer;
            LinkedList[] orderedLists = new LinkedList[shardSize];
            CountDownLatch countDownLatch = new CountDownLatch(shardSize);
            for (int i = 0; i < shardSize; i++) {
                orderedLists[i] = new LinkedList<ParallelOrderByExportEvent>();
                orderByExportProducer = new LocalOrderByExportProducer(druid, topologyList.get(i),
                    tableFieldMetaInfo, orderedLists[i], config.getOrderByColumnNameList(),
                    countDownLatch);
                executor.submit(orderByExportProducer);
            }
            ParallelMergeExportConsumer consumer;
            switch (config.getExportWay()) {
            case MAX_LINE_NUM_IN_SINGLE_FILE:
                consumer = new ParallelMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedLists, config.getLimitNum());
                break;
            case FIXED_FILE_NUM:
                // 固定文件数的情况 先拿到全部的行数
                double totalRowCount = DbUtil.getTableRowCount(druid.getConnection(), command.getTableName());
                int fileNum = config.getLimitNum();
                int singleLineLimit = (int) Math.ceil(totalRowCount / fileNum);
                // 再转为限制单文件行数的形式
                consumer = new ParallelMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedLists, singleLineLimit);
                break;
            case DEFAULT:
                consumer = new ParallelMergeExportConsumer(command.getFilePathPrefix(),
                    config.getSeparator(), orderByColumnInfoList, orderedLists, 0);
                break;
            default:
                throw new RuntimeException("Unsupported export exception");
            }
            try {
                // 等待生产者把数据全部buffer到内存
                countDownLatch.await();
                consumer.consume();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executor.shutdown();
            logger.info("导出 {} 数据完成", command.getTableName());
        } catch (DatabaseException | SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }
}
