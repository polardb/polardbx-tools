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
import cmd.ImportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import model.config.QuoteEncloseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.insert.ImportConsumer;
import worker.insert.ProcessOnlyImportConsumer;
import worker.insert.ShardedImportConsumer;

public class ImportExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ImportExecutor.class);

    private ImportCommand command;

    public ImportExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (ImportCommand) baseCommand;
    }

    @Override
    public void execute() {
        configureFieldMetaInfo();

        // 决定是否要分片
        if (command.isShardingEnabled()) {
            doShardingImport();
        } else {
            doDefaultImport();
        }

        logger.info("导入数据到 {} 完成", tableName);
    }

    private void doDefaultImport() {
        if (consumerExecutionContext.isReadProcessFileOnly()) {
            // 测试读取文件的性能
            configureCommonContextAndRun(ProcessOnlyImportConsumer.class,
                producerExecutionContext, consumerExecutionContext);
        } else {
            configureCommonContextAndRun(ImportConsumer.class,
                producerExecutionContext, consumerExecutionContext,
                producerExecutionContext.getQuoteEncloseMode() != QuoteEncloseMode.FORCE);
        }
    }

    private void doShardingImport() {
        configurePartitionKey();
        configureTopology();

        configureCommonContextAndRun(ShardedImportConsumer.class,
            producerExecutionContext, consumerExecutionContext,
            producerExecutionContext.getQuoteEncloseMode() != QuoteEncloseMode.FORCE);
    }
}
