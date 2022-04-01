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

package exec.export;

import cmd.BaseOperateCommand;
import cmd.ExportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.config.ExportConfig;
import model.config.GlobalVar;
import model.db.FieldMetaInfo;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.MyThreadPool;
import worker.export.order.DirectOrderByExportWorker;
import worker.export.order.LocalOrderByExportProducer;
import worker.export.order.OrderByExportEvent;
import worker.export.order.OrderByExportProducer;
import worker.export.order.OrderByMergeExportConsumer;
import worker.export.order.ParallelMergeExportConsumer;
import worker.export.order.ParallelOrderByExportEvent;
import worker.factory.ExportWorkerFactory;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static model.config.ConfigConstant.APP_NAME;

public class OrderByExportExecutor extends BaseExportExecutor {
    private static final Logger logger = LoggerFactory.getLogger(OrderByExportExecutor.class);

    private ExportCommand command;
    private ExportConfig config;

    public OrderByExportExecutor(DataSourceConfig dataSourceConfig,
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
    void exportData() {
        handleExportOrderBy();
    }

    /**
     * 根据配置的算法模式
     * 按指定字段排序排序导出
     */
    private void handleExportOrderBy() {
        if (!config.isLocalMerge()) {
            handleExportWithOrderByFromDb();
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
     * 带有order by的导出命令处理
     * 在本地进行merge
     */
    private void doExportWithOrderByLocal() {
        List<TableTopology> topologyList;
        ExportConfig config = command.getExportConfig();
        List<FieldMetaInfo> orderByColumnInfoList;
        try {
            topologyList = DbUtil.getTopology(dataSource.getConnection(), command.getTableName());
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(dataSource.getConnection(),
                getSchemaName(), command.getTableName());
            orderByColumnInfoList = DbUtil.getFieldMetaInfoListByColNames(dataSource.getConnection(), getSchemaName(),
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
                orderByExportProducer = new OrderByExportProducer(dataSource, topologyList.get(i),
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
                double totalRowCount = DbUtil.getTableRowCount(dataSource.getConnection(), command.getTableName());
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
     * 交给DB全局排序
     */
    private void handleExportWithOrderByFromDb() {
        try {
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(dataSource.getConnection(),
                getSchemaName(), command.getTableName());
            DirectOrderByExportWorker directOrderByExportWorker = ExportWorkerFactory
                .buildDirectExportWorker(dataSource, tableFieldMetaInfo, command);
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
            topologyList = DbUtil.getTopology(dataSource.getConnection(), command.getTableName());
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(dataSource.getConnection(),
                getSchemaName(), command.getTableName());
            orderByColumnInfoList = DbUtil.getFieldMetaInfoListByColNames(dataSource.getConnection(), getSchemaName(),
                command.getTableName(), config.getOrderByColumnNameList());
            // 分片数
            final int shardSize = topologyList.size();
            ExecutorService executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, shardSize);
            LocalOrderByExportProducer orderByExportProducer;
            LinkedList[] orderedLists = new LinkedList[shardSize];
            CountDownLatch countDownLatch = new CountDownLatch(shardSize);
            for (int i = 0; i < shardSize; i++) {
                orderedLists[i] = new LinkedList<ParallelOrderByExportEvent>();
                orderByExportProducer = new LocalOrderByExportProducer(dataSource, topologyList.get(i),
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
                double totalRowCount = DbUtil.getTableRowCount(dataSource.getConnection(), command.getTableName());
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
