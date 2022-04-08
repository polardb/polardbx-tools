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
import cmd.UpdateCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BaseWorkHandler;
import worker.update.ReplaceConsumer;
import worker.update.ShardedReplaceConsumer;
import worker.update.UpdateConsumer;
import worker.update.UpdateWithFuncConsumer;
import worker.update.UpdateWithFuncInConsumer;
import worker.util.UpdateUtil;

public class UpdateExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(UpdateExecutor.class);

    private UpdateCommand command;

    public UpdateExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (UpdateCommand) baseCommand;
    }

    @Override
    public void execute() {
        configureFieldMetaInfo();
        configurePkList();

        if (command.getConsumerExecutionContext().isFuncSqlForUpdateEnabled()) {
            // 启用函数则优先
            doUpdateWithFunc();
            logger.info("更新 {} 数据完成", tableNames);
            return;
        }
        if (!StringUtils.isEmpty(command.getConsumerExecutionContext().getWhereCondition())) {
            // 有where子句用默认方法
            doDefaultUpdate(UpdateConsumer.class);
            logger.info("更新 {} 数据完成", tableNames);
            return;
        }

        if (command.isShardingEnabled()) {
            doShardingUpdate();
        } else {
            // 禁用sharding则默认
            doDefaultUpdate(ReplaceConsumer.class);
        }
        logger.info("更新 {} 数据完成", tableNames);
    }

    private void doShardingUpdate() {
        configureTopology();
        configurePartitionKey();
        for (String tableName : tableNames) {
            String toUpdateColumns = UpdateUtil.formatToReplaceColumns(consumerExecutionContext.getTableFieldMetaInfo(tableName));
            consumerExecutionContext.setToUpdateColumns(toUpdateColumns);
            configureCommonContextAndRun(ShardedReplaceConsumer.class, producerExecutionContext,
                consumerExecutionContext, tableName);
        }
    }

    /**
     * 使用mysql函数进行字段更新
     */
    private void doUpdateWithFunc() {
        for (String tableName : tableNames) {
            String updateWithFuncSqlPattern = UpdateUtil.getUpdateWithFuncSqlPattern(tableName,
                consumerExecutionContext.getTableFieldMetaInfo(tableName).getFieldMetaInfoList(),
                consumerExecutionContext.getTablePkIndexSet(tableName));
            consumerExecutionContext.setUpdateWithFuncPattern(updateWithFuncSqlPattern);

            if (consumerExecutionContext.isWhereInEnabled()) {
                configureCommonContextAndRun(UpdateWithFuncInConsumer.class, producerExecutionContext,
                    consumerExecutionContext, tableName);
            } else {
                configureCommonContextAndRun(UpdateWithFuncConsumer.class, producerExecutionContext,
                    consumerExecutionContext, tableName);
            }
        }
    }

    /**
     * 无sharding
     *
     * @param clazz 决定实际的worker是使用update还是replace
     */
    private void doDefaultUpdate(Class<? extends BaseWorkHandler> clazz) {
        for (String tableName : tableNames) {
            String toUpdateColumns = UpdateUtil.formatToReplaceColumns(consumerExecutionContext.getTableFieldMetaInfo(tableName));
            consumerExecutionContext.setToUpdateColumns(toUpdateColumns);

            configureCommonContextAndRun(clazz, producerExecutionContext,
                consumerExecutionContext, tableName);
        }
    }
}
