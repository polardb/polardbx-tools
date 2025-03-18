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
import cmd.WriteDbCommand;
import com.alibaba.druid.pool.DruidDataSource;
import com.lmax.disruptor.WorkerPool;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import model.db.PartitionKey;
import model.db.PrimaryKey;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CountStat;
import util.DbUtil;
import worker.common.BaseWorkHandler;
import worker.common.ReadFileWithBlockProducer;
import worker.tpch.model.TpchTableModel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实现import/update/delete的公用方法
 */
public abstract class WriteDbExecutor extends BaseExecutor {
    private static final Logger logger = LoggerFactory.getLogger(WriteDbExecutor.class);

    protected ProducerExecutionContext producerExecutionContext;
    protected ConsumerExecutionContext consumerExecutionContext;
    protected List<String> tableNames;

    public WriteDbExecutor(DataSourceConfig dataSourceConfig, DruidDataSource druid,
                           BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
        WriteDbCommand writeDbCommand = (WriteDbCommand) baseCommand;
        this.producerExecutionContext = writeDbCommand.getProducerExecutionContext();
        this.consumerExecutionContext = writeDbCommand.getConsumerExecutionContext();
        this.tableNames = consumerExecutionContext.getTableNames();
    }

    /**
     * 设置主键
     */
    protected void configurePkList() {
        Map<String, List<PrimaryKey>> tablePkList = new HashMap<>();
        for (String tableName : tableNames) {
            List<PrimaryKey> pkList = null;
            try (Connection connection = dataSource.getConnection()) {
                pkList = DbUtil.getPkList(connection, getSchemaName(), tableName);
                tablePkList.put(tableName, pkList);
            } catch (DatabaseException | SQLException e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        consumerExecutionContext.setTablePkList(tablePkList);
    }

    @Override
    protected void printStatLog() {
        logger.info("当前导入行数：{}", CountStat.getDbRowCount());
    }

    /**
     * 设置字段信息
     */
    protected void configureFieldMetaInfo() {
        logger.info("正在获取所有表的元信息...");
        Map<String, TableFieldMetaInfo> tableFieldMetaInfoMap = null;
        try (Connection connection = dataSource.getConnection()) {
            if (command.getColumnNames() != null) {
                assert tableNames.size() == 1;
                tableFieldMetaInfoMap = new HashMap<>();
                TableFieldMetaInfo fieldMetaInfo = DbUtil.getTableFieldMetaInfo(connection, getSchemaName(),
                    tableNames.get(0), command.getColumnNames());
                tableFieldMetaInfoMap.put(tableNames.get(0), fieldMetaInfo);
            } else {
                tableFieldMetaInfoMap = DbUtil.getDbFieldMetaInfo(connection,
                    getSchemaName(), tableNames);
            }
        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        consumerExecutionContext.setTableFieldMetaInfo(tableFieldMetaInfoMap);
        logger.info("所有表的元信息获取完毕");
    }

    /**
     * 设置拓扑信息
     */
    protected void configureTopology() {
        Map<String, List<TableTopology>> tableTopologyMap = new HashMap<>();
        for (String tableName : tableNames) {
            List<TableTopology> topologyList = null;
            try (Connection connection = dataSource.getConnection()) {
                topologyList = DbUtil.getTopology(connection, tableName);
                tableTopologyMap.put(tableName, topologyList);
            } catch (DatabaseException | SQLException e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        consumerExecutionContext.setTopologyList(tableTopologyMap);
    }

    protected void configurePartitionKey() {
        Map<String, PartitionKey> tablePartitionKey = new HashMap<>();
        for (String tableName : tableNames) {
            PartitionKey partitionKey;
            try (Connection connection = dataSource.getConnection()) {
                partitionKey = DbUtil.getPartitionKey(connection,
                    getSchemaName(), tableName);
                logger.info("表 {} 使用分片键 {}", tableName, partitionKey);
                tablePartitionKey.put(tableName, partitionKey);
            } catch (DatabaseException | SQLException e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        consumerExecutionContext.setTablePartitionKey(tablePartitionKey);
    }

    /**
     * 检查进度，记录断点续传点
     */
    @Override
    protected void checkConsumeProgress(ReadFileWithBlockProducer producers, BaseWorkHandler[] consumers) {
        ScheduledThreadPoolExecutor checkConsumePartFinishScheduler = new ScheduledThreadPoolExecutor(1,
            r -> new Thread(r, "[check-progress-thread]"));
        checkConsumePartFinishScheduler.scheduleAtFixedRate(() -> {
            AtomicBoolean[] produceProgress = producers.getFileDoneList();
            long nextDoBlockIndex;
            for (int i = producerExecutionContext.getNextFileIndex(); i < produceProgress.length; ++i) {
                nextDoBlockIndex = Long.MAX_VALUE;
                ConcurrentHashMap<Long, AtomicInteger> fileDone =
                    producerExecutionContext.getEventCounter().get(i);
                for (ConcurrentHashMap.Entry<Long, AtomicInteger> entry : fileDone.entrySet()) {
                    if (entry.getValue().get() > 0) {
                        nextDoBlockIndex = nextDoBlockIndex > entry.getKey() ?
                            entry.getKey() : nextDoBlockIndex;
                    }
                }
                if (nextDoBlockIndex == Long.MAX_VALUE) {
                    // means now nextDoFileIndex consume over
                    if (produceProgress[i].get()) {
                        // means all file consume over
                        if (i + 1 == produceProgress.length) {
                            logger.info("所有文件处理完毕");
                        }
                    } else {
                        producerExecutionContext.setNextFileIndex(i);
                        producerExecutionContext.setNextBlockIndex(0);
                        break;
                    }
                } else {
                    producerExecutionContext.setNextFileIndex(i);
                    producerExecutionContext.setNextBlockIndex(nextDoBlockIndex);
                    break;
                }
            }
            producerExecutionContext.saveToHistoryFile(false);
            logger.info("下一个文件 {}", producerExecutionContext.getNextFileIndex());
            logger.info("下一数据块 {}", producerExecutionContext.getNextBlockIndex());
        }, 30, 60, TimeUnit.SECONDS);
        checkConsumePartFinishScheduler.shutdown();
    }

    @Override
    protected void onWorkFinished() {
        producerExecutionContext.saveToHistoryFile(true);
    }

    @Override
    public boolean hasFatalException() {
        return producerExecutionContext.getException() != null || consumerExecutionContext.getException() != null;
    }

    protected boolean useBlockReader() {
        if (GlobalVar.IN_PERF_MODE) {
            return true;
        }
        if (producerExecutionContext.getQuoteEncloseMode() == QuoteEncloseMode.FORCE) {
            return false;
        }
        if (!producerExecutionContext.getEncryptionConfig().getEncryptionMode().isSupportStreamingBit()) {
            return false;
        }
        if (!producerExecutionContext.getFileFormat().isSupportBlock()) {
            return false;
        }
        return true;
    }

    protected void waitAndShutDown(CountDownLatch countDownLatch, AtomicInteger emittedDataCounter,
                                   ThreadPoolExecutor producerThreadPool, ThreadPoolExecutor consumerThreadPool,
                                   WorkerPool<?> workerPool) {
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

    protected void checkTpchUpdateTablesExist() {
        final String[] tables = {TpchTableModel.LINEITEM.getName(), TpchTableModel.ORDERS.getName()};
        try (Connection conn = dataSource.getConnection()) {
            for (String table : tables) {
                if (!DbUtil.checkTableExists(conn, table)) {
                    throw new RuntimeException("TPC-H update table: " + table + " does not exist");
                }
            }
        } catch (SQLException | DatabaseException e) {
            throw new RuntimeException(e);
        }
    }
}
