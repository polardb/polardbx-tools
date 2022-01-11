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

import model.db.PartitionKey;
import model.db.PrimaryKey;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerExecutionContext {

    private DataSource dataSource;

    private String tableName;
    /**
     * 分隔符
     */
    private String sep;

    private String charset;
    /**
     * 对于已发送数据批的计数器
     */
    private AtomicInteger emittedDataCounter;

    private List<PrimaryKey> pkList;

    /**
     * 主键序号集合
     */
    private Set<Integer> pkIndexSet;
    /**
     * 数据表元信息
     */
    private TableFieldMetaInfo tableFieldMetaInfo;
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
     * 是否强制使用指定的并发度，忽略cpu的实际core数目
     */
    private boolean forceParallelism;

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
     * 分库分表拓扑结构
     */
    private List<TableTopology> topologyList;
    /**
     * 划分键
     */
    private PartitionKey partitionKey;
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

    private String pkNames;

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

    private double batchTpsLimitPerConsumer;

    private List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter;

    private boolean isUsingBlock = true;

    public String getSep() {
        return sep;
    }

    public void setSep(String sep) {
        this.sep = sep;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
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

    public List<PrimaryKey> getPkList() {
        return pkList;
    }

    public void setPkList(List<PrimaryKey> pkList) {
        this.pkList = pkList;
        pkIndexSet = new HashSet<>(pkList.size() * 2);
        StringBuilder stringBuilder = new StringBuilder();
        for (PrimaryKey primaryKey : pkList) {
            pkIndexSet.add(primaryKey.getOrdinalPosition() - 1);
            stringBuilder.append(primaryKey.getName()).append(",");
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        pkNames = stringBuilder.toString();
    }

    public TableFieldMetaInfo getTableFieldMetaInfo() {
        return tableFieldMetaInfo;
    }

    public void setTableFieldMetaInfo(TableFieldMetaInfo tableFieldMetaInfo) {
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Set<Integer> getPkIndexSet() {
        return pkIndexSet;
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

    public List<TableTopology> getTopologyList() {
        return topologyList;
    }

    public void setTopologyList(List<TableTopology> topologyList) {
        this.topologyList = topologyList;
    }

    public PartitionKey getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(PartitionKey partitionKey) {
        this.partitionKey = partitionKey;
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
            "tableName='" + tableName + '\'' +
            ", sep='" + sep + '\'' +
            ", charset='" + charset + '\'' +
            ", pkList=" + pkList +
            ", pkIndexSet=" + pkIndexSet +
            ", tableFieldMetaInfo=" + tableFieldMetaInfo +
            ", insertIgnoreAndResumeEnabled=" + insertIgnoreAndResumeEnabled +
            ", funcSqlForUpdateEnabled=" + funcSqlForUpdateEnabled +
            ", parallelism=" + parallelism +
            ", whereCondition='" + whereCondition + '\'' +
            ", toUpdateColumns='" + toUpdateColumns + '\'' +
            ", topologyList=" + topologyList +
            ", partitionKey=" + partitionKey +
            ", updateWithFuncPattern='" + updateWithFuncPattern + '\'' +
            ", sqlEscapeEnabled=" + sqlEscapeEnabled +
            ", readProcessFileOnly=" + readProcessFileOnly +
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

    public String getPkNames() {
        return pkNames;
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

    public boolean isForceParallelism() {
        return forceParallelism;
    }

    public void setForceParallelism(boolean forceParallelism) {
        this.forceParallelism = forceParallelism;
    }

    public boolean isUsingBlock() {
        return isUsingBlock;
    }

    public void setUsingBlock(boolean usingBlock) {
        isUsingBlock = usingBlock;
    }
}
