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
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CountStat;
import util.DbUtil;
import util.FileUtil;
import worker.MyThreadPool;
import worker.export.DirectExportWorker;
import worker.factory.ExportWorkerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static model.config.ConfigConstant.APP_NAME;

public class SingleThreadExportExecutor extends BaseExportExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SingleThreadExportExecutor.class);

    private ExecutorService executor = null;
    private CountDownLatch countDownLatch = null;

    public SingleThreadExportExecutor(DataSourceConfig dataSourceConfig,
                                      DruidDataSource druid,
                                      BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    /**
     * 每张表对应一个线程进行导出
     */
    @Override
    void exportData() {
        List<String> tableNames = command.getTableNames();
        this.executor = MyThreadPool.createExecutorWithEnsure(APP_NAME, tableNames.size());
        this.countDownLatch = new CountDownLatch(tableNames.size());
        for (String tableName : tableNames) {
            CountStat.getTableRowCount(tableName);
        }
        startStatLog();
        for (String tableName : tableNames) {
            try {
                handleSingleTable(tableName);
            } catch (Exception e) {
                logger.error("导出 {} 数据失败：{}", tableName, e.getMessage());
            }
        }
        try {
            countDownLatch.await();
            executor.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        stopStatLog();
    }

    /**
     * 使用单条长连接导出数据
     */
    private void doDefaultExport(String tableName, ExecutorService executor,
                                 CountDownLatch countDownLatch) {
        String fileName = FileUtil.getFilePathPrefix(config.getPath(),
            config.getFilenamePrefix(), tableName) + 0;
        try (Connection connection = dataSource.getConnection()) {
            TableFieldMetaInfo tableFieldMetaInfo = DbUtil.getTableFieldMetaInfo(connection,
                getSchemaName(), tableName, command.getColumnNames());
            DirectExportWorker directExportWorker = ExportWorkerFactory.buildDefaultDirectExportWorker(dataSource,
                tableName, new TableTopology(tableName), tableFieldMetaInfo,
                fileName, config);
            directExportWorker.setCountDownLatch(countDownLatch);
            executor.submit(directExportWorker);
            logger.info("开始导出表 {} 到文件 {}", tableName, fileName);
        } catch (DatabaseException | SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    protected void handleSingleTableInner(String tableName) {
        doDefaultExport(tableName, executor, countDownLatch);
    }

    @Override
    protected void beforeSingleTable(String tableName) {
        // do nothing since it is async
    }

    @Override
    protected void afterSingleTable(String tableName) {
        // do nothing since it is async
    }

    @Override
    protected void printStatLog() {
        List<String> tableNames = command.getTableNames();
        for (String tableName : tableNames) {
            logger.info("表 {} 当前导出行数：{}", tableName, CountStat.getTableRowCount(tableName));
        }
    }
}
