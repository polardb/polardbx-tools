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
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import datasource.DruidSource;
import exec.BaseExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class BatchTool {
    private static final Logger logger = LoggerFactory.getLogger(BatchTool.class);

    private DruidDataSource druid;

    private static final BatchTool instance = new BatchTool();

    private BatchTool() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::destroy));
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
    }

    private void destroy() {
        if (druid != null) {
            druid.close();
        }
    }
}
