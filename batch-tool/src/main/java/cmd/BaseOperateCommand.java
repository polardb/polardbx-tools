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

package cmd;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * 数据库操作相关配置
 */
public class BaseOperateCommand {

    private final String dbName;

    /**
     * 支持同一个库下的多张表
     */
    private List<String> tableNames;

    /**
     * 仅支持单张表下指定列
     */
    private List<String> columnNames;

    /**
     * 是否开启分库分表操作
     */
    private boolean shardingEnabled;

    public BaseOperateCommand(@NotNull String dbName, boolean shardingEnabled) {
        this.dbName = dbName;
        this.shardingEnabled = shardingEnabled;
    }

    public BaseOperateCommand(@NotNull String dbName, List<String> tableName, boolean shardingEnabled) {
        this.dbName = dbName;
        this.tableNames = tableName;
        this.shardingEnabled = shardingEnabled;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public boolean isSingleTable() {
        return this.tableNames.size() == 1;
    }

    public boolean isShardingEnabled() {
        return shardingEnabled;
    }

    public void setShardingEnabled(boolean shardingEnabled) {
        this.shardingEnabled = shardingEnabled;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean isDbOperation() {
        return this.tableNames == null;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}
