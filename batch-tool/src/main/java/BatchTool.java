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

import cmd.BaseOperateCommand;
import cmd.ExportCommand;
import cmd.WriteDbCommand;
import com.alibaba.druid.pool.DruidDataSource;
import com.lmax.disruptor.RingBuffer;
import datasource.DataSourceConfig;
import datasource.DruidSource;
import exec.BaseExecutor;
import model.config.GlobalVar;
import model.stat.DebugInfo;
import model.stat.FileReaderStat;
import model.stat.SqlStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.sql.SQLException;
import java.util.List;

import static model.config.ConfigConstant.DEBUG_SIGNAL;

public class BatchTool {
    private static final Logger logger = LoggerFactory.getLogger(BatchTool.class);

    private DruidDataSource druid;
    private volatile BaseOperateCommand command = null;

    private static final BatchTool instance = new BatchTool();

    private BatchTool() {
        addHooks();
    }

    public static BatchTool getInstance() {
        return instance;
    }

    public void initDatasource(DataSourceConfig dataSourceConfig) throws SQLException {
        logger.info("连接数据库信息: {}", dataSourceConfig);
        DruidSource.setDataSourceConfig(dataSourceConfig);
        this.druid = DruidSource.getInstance();
        this.druid.init();
        logger.info("连接数据库成功");
    }


    public void doBatchOp(BaseOperateCommand command, DataSourceConfig dataSourceConfig) {
        logger.info("开始批量操作...");
        this.command = command;
        BaseExecutor commandExecutor = BaseExecutor.getExecutor(command, dataSourceConfig, druid);
        commandExecutor.preCheck();
        logger.info(command.toString());
        try {
            long startTime = System.currentTimeMillis();
            commandExecutor.execute();
            long endTime = System.currentTimeMillis();
            logger.info("运行耗时： {} s", (endTime - startTime) / 1000F);
        } finally {
            commandExecutor.close();
        }
        if (commandExecutor.hasFatalException()) {
            throw new RuntimeException("Fatal exception occurred during batch operation.");
        }
    }

    private void addHooks() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::destroy));
        try {
            Signal.handle(DEBUG_SIGNAL, sig -> {
                printDebugInfo();
            });
        } catch (Exception e) {
            logger.info("Failed to register signal handler(this can be ignored): {}", e.getMessage());
        }
    }

    private void printDebugInfo() {
        logger.warn("Collecting debug info...");
        if (this.druid != null) {
            logger.warn("[Druid] activeCount:{}, poolingCount:{}.",
                druid.getActiveCount(), druid.getPoolingCount());
        }
        if (this.command == null) {
            logger.warn("Batch operation is not started yet!");
            return;
        }
        DebugInfo debugInfo = GlobalVar.DEBUG_INFO;
        RingBuffer ringBuffer = debugInfo.getRingBuffer();
        if (ringBuffer != null) {
            logger.warn("[RingBuffer] size: {}, available: {}.",
                ringBuffer.getBufferSize(), ringBuffer.remainingCapacity());
        }
        if (this.command instanceof ExportCommand) {
            logger.warn("Detailed debug info of export operation is not supported yet!");
        } else if (this.command instanceof WriteDbCommand) {
            if (debugInfo.getCountDownLatch() != null && debugInfo.getRemainDataCounter() != null) {
                logger.warn("[Counter] countDownLatch: {}, remainDataCount: {}.",
                    debugInfo.getCountDownLatch().getCount(), debugInfo.getRemainDataCounter().get());
            }

            List<FileReaderStat> fileReaderStatList = debugInfo.getFileReaderStatList();
            if (!fileReaderStatList.isEmpty()) {
                long totalCount = 0;
                for (FileReaderStat fileReaderStat : fileReaderStatList) {
                    totalCount += fileReaderStat.getCount();
                }
                logger.warn("[Producer] totalCount: {}.", totalCount);
            }

            List<SqlStat> sqlStatList = debugInfo.getSqlStatList();
            if (!sqlStatList.isEmpty()) {
                SqlStat firstStat = sqlStatList.get(0);
                double totalAvg = firstStat.getAvgTimeMillis();
                double minAvg = totalAvg, maxAvg = totalAvg;
                for (int i = 1; i < sqlStatList.size(); i++) {
                    double avg = sqlStatList.get(i).getAvgTimeMillis();
                    totalAvg += avg;
                    minAvg = Math.min(minAvg, avg);
                    maxAvg = Math.max(maxAvg, avg);
                }
                double avgConsumerAvg = totalAvg / sqlStatList.size();
                logger.warn("[Consumer] avgSqlRtAvg: {}ms, avgSqlRtMin: {}ms, avgSqlRtMax: {}ms.",
                    avgConsumerAvg, minAvg, maxAvg);
            }

        }
        logger.warn("End of debug info.");
    }

    private void destroy() {
        if (druid != null) {
            druid.close();
        }
    }
}
