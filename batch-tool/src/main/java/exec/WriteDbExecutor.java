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
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.db.PartitionKey;
import model.db.PrimaryKey;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.common.BaseWorkHandler;
import worker.common.ReadFileWithBlockProducer;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
    protected String tableName;

    public WriteDbExecutor(DataSourceConfig dataSourceConfig, DruidDataSource druid,
                           BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
        WriteDbCommand writeDbCommand = (WriteDbCommand) baseCommand;
        this.producerExecutionContext = writeDbCommand.getProducerExecutionContext();
        this.consumerExecutionContext = writeDbCommand.getConsumerExecutionContext();
        this.tableName = consumerExecutionContext.getTableName();
    }

    /**
     * 设置主键
     */
    protected void configurePkList() {
        List<PrimaryKey> pkList = null;
        try {
            pkList = DbUtil.getPkList(dataSource.getConnection(), getSchemaName(), tableName);
        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        consumerExecutionContext.setPkList(pkList);
    }

    /**
     * 设置字段信息
     */
    protected void configureFieldMetaInfo() {
        TableFieldMetaInfo tableFieldMetaInfo = null;
        try {
            tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(dataSource.getConnection(),
                getSchemaName(), tableName);
        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        consumerExecutionContext.setTableFieldMetaInfo(tableFieldMetaInfo);
    }

    /**
     * 设置拓扑信息
     */
    protected void configureTopology() {
        List<TableTopology> topologyList = null;
        try {
            topologyList = DbUtil.getTopology(dataSource.getConnection(), tableName);
        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        consumerExecutionContext.setTopologyList(topologyList);
    }

    protected void configurePartitionKey() {
        PartitionKey partitionKey = null;
        try {
            partitionKey = DbUtil.getPartitionKey(dataSource.getConnection(),
                getSchemaName(), tableName);
            logger.info("使用划分键 {}", partitionKey);

        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        consumerExecutionContext.setPartitionKey(partitionKey);
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
                            logger.info("deal with all files over");
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
            logger.info("next file {}", producerExecutionContext.getNextFileIndex());
            logger.info("next block {}", producerExecutionContext.getNextBlockIndex());
        }, 30, 60, TimeUnit.SECONDS);
        checkConsumePartFinishScheduler.shutdown();
    }

    @Override
    protected void onWorkFinished() {
        producerExecutionContext.saveToHistoryFile(true);
    }

}
