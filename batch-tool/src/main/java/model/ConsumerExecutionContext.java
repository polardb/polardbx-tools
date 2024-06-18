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

package model;

import model.config.BaseConfig;
import model.config.ConfigConstant;
import model.db.PartitionKey;
import model.db.PrimaryKey;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 连接数据库端的工作线程上下文
 */
public class ConsumerExecutionContext extends BaseConfig {

    private DataSource dataSource;

    private List<String> tableNames;

    /**
     * 对于已发送数据批的计数器
     */
    private AtomicInteger emittedDataCounter;

    private Map<String, List<PrimaryKey>> tablePkList;

    /**
     * 主键序号集合
     */
    private Map<String, Set<Integer>> tablePkIndexSet;
    /**
     * 数据表元信息
     */
    private Map<String, TableFieldMetaInfo> tableFieldMetaInfo;
    /**
     * 是否开启insert ignore & resume breakpoint
     */
    private boolean insertIgnoreAndResumeEnabled;
    /**
     * 是否开启sql使用函数更新
     */
    private boolean funcSqlForUpdateEnabled;
    /**
     * 消费者并发度
     */
    private int parallelism;

    /**
     * where 条件
     */
    private String whereCondition;

    /**
     * 更新列名
     * 以 , 分割
     */
    private String toUpdateColumns;

    /**
     * 物理库表拓扑结构
     */
    private Map<String, List<TableTopology>> topologyList;
    /**
     * 划分键
     */
    private Map<String, PartitionKey> tablePartitionKey;
    /**
     * update tableName set x=2x,y=2y,str=REVERSE(str) where %s;
     */
    private String updateWithFuncPattern;

    /**
     * 是否启用字符串的sql转义
     */
    private boolean sqlEscapeEnabled = true;
    /**
     * 只读取与处理文件
     */
    private boolean readProcessFileOnly = false;
    /**
     * 删除和更新时使用
     * where 主键 in (...)
     * 的语句
     */
    private boolean whereInEnabled = false;

    private Map<String, String> tablePkName;

    /**
     * 文件每行的结尾是否为分隔符
     */
    private boolean isWithLastSep;
    /**
     * 是否为并行归并
     */
    private boolean isParallelMerge;

    /**
     * 限流
     */
    private int tpsLimit;

    /**
     * 字符串类型字段，空值视作NULL
     */
    private boolean emptyStrAsNull = false;

    private double batchTpsLimitPerConsumer;

    private List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter;

    private boolean useBlock = true;

    private boolean useMagicSeparator = false;

    /**
     * 以逗号拼接的指定使用列
     */
    private String useColumns = null;

    private int maxRetry;

    private volatile Exception exception;

    public ConsumerExecutionContext() {
        super(ConfigConstant.DEFAULT_IMPORT_SHARDING_ENABLED);
    }

    public List<ConcurrentHashMap<Long, AtomicInteger>> getEventCounter() {
        return eventCounter;
    }

    public void setEventCounter(List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter) {
        this.eventCounter = eventCounter;
    }

    public AtomicInteger getEmittedDataCounter() {
        return emittedDataCounter;
    }

    public void setEmittedDataCounter(AtomicInteger emittedDataCounter) {
        this.emittedDataCounter = emittedDataCounter;
    }

    public Map<String, List<PrimaryKey>> getTablePkList() {
        return tablePkList;
    }

    public List<PrimaryKey> getTablePkList(String tableName) {
        return tablePkList.get(tableName);
    }

    public void setTablePkList(Map<String, List<PrimaryKey>> tablePkList) {
        this.tablePkList = tablePkList;
        this.tablePkIndexSet = new HashMap<>(tablePkList.size() * 2);
        this.tablePkName = new HashMap<>(tablePkList.size() * 2);
        for (Map.Entry<String, List<PrimaryKey>> tablePk : tablePkList.entrySet()) {
            Set<Integer> pkSet = new HashSet<>();
            StringBuilder stringBuilder = new StringBuilder();
            for (PrimaryKey primaryKey : tablePk.getValue()) {
                pkSet.add(primaryKey.getOrdinalPosition() - 1);
                stringBuilder.append(primaryKey.getName()).append(",");
            }
            if (stringBuilder.length() > 0) {
                stringBuilder.setLength(stringBuilder.length() - 1);
            }
            this.tablePkIndexSet.put(tablePk.getKey(), pkSet);
            this.tablePkName.put(tablePk.getKey(), stringBuilder.toString());
        }
    }

    public Map<String, TableFieldMetaInfo> getTableFieldMetaInfo() {
        return tableFieldMetaInfo;
    }

    public TableFieldMetaInfo getTableFieldMetaInfo(String tableName) {
        return tableFieldMetaInfo.get(tableName);
    }

    public void setTableFieldMetaInfo(Map<String, TableFieldMetaInfo> tableFieldMetaInfo) {
        this.tableFieldMetaInfo = tableFieldMetaInfo;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isInsertIgnoreAndResumeEnabled() {
        return insertIgnoreAndResumeEnabled;
    }

    public void setInsertIgnoreAndResumeEnabled(boolean insertIgnoreAndResumeEnabled) {
        this.insertIgnoreAndResumeEnabled = insertIgnoreAndResumeEnabled;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Map<String, Set<Integer>> getTablePkIndexSet() {
        return tablePkIndexSet;
    }

    public Set<Integer> getTablePkIndexSet(String tableName) {
        return tablePkIndexSet.get(tableName);
    }

    public String getToUpdateColumns() {
        return toUpdateColumns;
    }

    public void setToUpdateColumns(String toUpdateColumns) {
        this.toUpdateColumns = toUpdateColumns;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    public Map<String, List<TableTopology>> getTopologyList() {
        return topologyList;
    }

    public List<TableTopology> getTopologyList(String tableName) {
        return topologyList.get(tableName);
    }

    public void setTopologyList(Map<String, List<TableTopology>> topologyList) {
        this.topologyList = topologyList;
    }

    public Map<String, PartitionKey> getTablePartitionKey() {
        return tablePartitionKey;
    }

    public PartitionKey getTablePartitionKey(String tableName) {
        return tablePartitionKey.get(tableName);
    }

    public void setTablePartitionKey(Map<String, PartitionKey> tablePartitionKey) {
        this.tablePartitionKey = tablePartitionKey;
    }

    public int getTpsLimit() {
        return tpsLimit;
    }

    public void setTpsLimit(int tpsLimit) {
        this.tpsLimit = tpsLimit;
    }

    public double getBatchTpsLimitPerConsumer() {
        return batchTpsLimitPerConsumer;
    }

    public void setBatchTpsLimitPerConsumer(double batchTpsLimitPerConsumer) {
        this.batchTpsLimitPerConsumer = batchTpsLimitPerConsumer;
    }

    @Override
    public String toString() {
        return "ConsumerExecutionContext{" +
            "tableNames=" + tableNames +
//            ", insertIgnoreAndResumeEnabled=" + insertIgnoreAndResumeEnabled +
            ", parallelism=" + parallelism +
            ", whereCondition='" + whereCondition + '\'' +
//            ", toUpdateColumns='" + toUpdateColumns + '\'' +
//            ", updateWithFuncPattern='" + updateWithFuncPattern + '\'' +
            ", sqlEscapeEnabled=" + sqlEscapeEnabled +
            '}';
    }

    public String getUpdateWithFuncPattern() {
        return updateWithFuncPattern;
    }

    public void setUpdateWithFuncPattern(String updateWithFuncPattern) {
        this.updateWithFuncPattern = updateWithFuncPattern;
    }

    public boolean isFuncSqlForUpdateEnabled() {
        return funcSqlForUpdateEnabled;
    }

    public void setFuncSqlForUpdateEnabled(boolean funcSqlForUpdateEnabled) {
        this.funcSqlForUpdateEnabled = funcSqlForUpdateEnabled;
    }

    public boolean isSqlEscapeEnabled() {
        return sqlEscapeEnabled;
    }

    public void setSqlEscapeEnabled(boolean sqlEscapeEnabled) {
        this.sqlEscapeEnabled = sqlEscapeEnabled;
    }

    public boolean isReadProcessFileOnly() {
        return readProcessFileOnly;
    }

    public void setReadProcessFileOnly(boolean readProcessFileOnly) {
        this.readProcessFileOnly = readProcessFileOnly;
    }

    public boolean isWhereInEnabled() {
        return whereInEnabled;
    }

    public void setWhereInEnabled(boolean whereInEnabled) {
        this.whereInEnabled = whereInEnabled;
    }

    public Map<String, String> getTablePkName() {
        return tablePkName;
    }

    public String getTablePkName(String tableName) {
        return tablePkName.get(tableName);
    }

    public boolean isWithLastSep() {
        return isWithLastSep;
    }

    public void setWithLastSep(boolean withLastSep) {
        isWithLastSep = withLastSep;
    }

    public boolean isParallelMerge() {
        return isParallelMerge;
    }

    public void setParallelMerge(boolean parallelMerge) {
        isParallelMerge = parallelMerge;
    }

    public boolean isUseBlock() {
        return useBlock;
    }

    public void setUseBlock(boolean useBlock) {
        this.useBlock = useBlock;
    }

    public boolean isUseMagicSeparator() {
        return useMagicSeparator;
    }

    public void setUseMagicSeparator(boolean useMagicSeparator) {
        this.useMagicSeparator = useMagicSeparator;
    }

    public boolean isSingleThread() {
        return this.parallelism == 1;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public String getUseColumns() {
        return useColumns;
    }

    public void setUseColumns(String useColumns) {
        this.useColumns = useColumns;
    }

    public boolean isEmptyStrAsNull() {
        return emptyStrAsNull;
    }

    public void setEmptyStrAsNull(boolean emptyStrAsNull) {
        this.emptyStrAsNull = emptyStrAsNull;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    @Override
    public void validate() {
        super.validate();
        if (useColumns != null) {
            if (tableNames == null) {
                throw new UnsupportedOperationException("Do not support db operation with specified columns");
            }
            if (tableNames.size() > 1) {
                throw new UnsupportedOperationException("Do not support multi-table operation with specified columns");
            }
        }
    }
}
