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
import cmd.CommandUtil;
import cmd.ConfigResult;
import datasource.DataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Version;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.sql.SQLException;
import java.util.List;

public class BatchToolLauncher {
    private static final Logger logger = LoggerFactory.getLogger(BatchTool.class);

    private static final BatchTool BATCH_TOOL_INSTANCE = BatchTool.getInstance();

    public static void main(String[] args) {
        if (args.length == 0) {
            CommandUtil.printHelp();
            return;
        }

        ConfigResult commandLine = CommandUtil.parseStartUpCommand(args);
        if (commandLine == null || CommandUtil.doHelpCmd(commandLine)) {
            return;
        }

        printStartInfo();
        try {
            handleCmd(commandLine);
        } catch (Throwable e) {
            // 主线程异常
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    private static void printStartInfo() {
        logger.info("BatchTool version: {}", Version.getVersion());
        try {
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            logger.info("Max heap size: {} MB. Initial heap size: {} MB", heapUsage.getMax() / (1024 * 1024),
                heapUsage.getInit() / (1024 * 1024));

            if (logger.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder(256);
                List<MemoryPoolMXBean> memoryPools = ManagementFactory.getMemoryPoolMXBeans();
                for (MemoryPoolMXBean memoryPool : memoryPools) {
                    String name = memoryPool.getName();
                    stringBuilder.append(name).append(": ").append(memoryPool.getUsage().getMax() / (1024 * 1024))
                        .append(" MB").append(". ");
                }
                logger.debug(stringBuilder.toString());
            }
        } catch (Exception e) {
            logger.debug(e.getMessage(), e);
        }
    }

    private static void handleCmd(ConfigResult commandLine) throws SQLException {
        DataSourceConfig dataSourceConfig;
        try {
            dataSourceConfig = CommandUtil.getDataSourceConfigFromCmd(commandLine);
            BATCH_TOOL_INSTANCE.initDatasource(dataSourceConfig);
        } catch (SQLException e) {
            logger.error("连接数据库失败");
            throw e;
        }

        try {
            BaseOperateCommand command = CommandUtil.getOperateCommandFromCmd(commandLine);
            BATCH_TOOL_INSTANCE.doBatchOp(command, dataSourceConfig);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
