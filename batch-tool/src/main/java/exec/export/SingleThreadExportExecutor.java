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
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import util.FileUtil;
import worker.MyThreadPool;
import worker.export.DirectExportWorker;
import worker.export.order.DirectOrderExportWorker;
import worker.factory.ExportWorkerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static model.config.ConfigConstant.APP_NAME;

public class SingleThreadExportExecutor extends BaseExportExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SingleThreadExportExecutor.class);

    public SingleThreadExportExecutor(DataSourceConfig dataSourceConfig,
                                      DruidDataSource druid,
                                      BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    void exportData() {
        doDefaultExport();
    }

    /**
     * 使用单条长连接导出数据
     */
    private void doDefaultExport() {
        List<String> tableNames = command.getTableNames();
        ExecutorService executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, tableNames.size());
        for (String tableName : tableNames) {
            String fileName = FileUtil.getFilePathPrefix(config.getPath(),
                config.getFilenamePrefix(), tableName) + 0;
            try {
                TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(dataSource.getConnection(),
                    getSchemaName(), tableName, command.getColumnNames());
                DirectExportWorker directExportWorker = ExportWorkerFactory.buildDefaultDirectExportWorker(dataSource,
                    new TableTopology("", tableName), tableFieldMetaInfo,
                    fileName, config);
                executor.submit(directExportWorker);
                logger.info("开始导出表 {} 到文件 {}", tableName, fileName);
            } catch (DatabaseException | SQLException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
        executor.shutdown();
    }
}
