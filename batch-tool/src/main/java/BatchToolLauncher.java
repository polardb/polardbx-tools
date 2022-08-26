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

import java.sql.SQLException;

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

        try {
            handleCmd(commandLine);
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
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

        BaseOperateCommand command = CommandUtil.getOperateCommandFromCmd(commandLine);
        BATCH_TOOL_INSTANCE.doBatchOp(command, dataSourceConfig);
    }
}
