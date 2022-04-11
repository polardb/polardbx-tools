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

package worker.common;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.druid.util.StringUtils;
import model.db.FieldMetaInfo;
import model.db.PartitionKey;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import util.FileUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static model.config.ConfigConstant.END_OF_BATCH_LINES;

/**
 * 根据分片处理的消费者
 */
public abstract class BaseShardedConsumer extends BaseWorkHandler {
    private static final Logger logger = LoggerFactory.getLogger(BaseShardedConsumer.class);

    protected void initLocalVars() {
        super.initLocalVars();
    }

    @Override
    public void onProxyEvent(BatchLineEvent event) {
        initLocalVars();
        try {
            List<TableTopology> topologyList = consumerContext.getTopologyList(tableName);
            List<FieldMetaInfo> fieldMetaInfoList = consumerContext.getTableFieldMetaInfo(tableName)
                .getFieldMetaInfoList();
            int shardCount = topologyList.size();
            // 分片序号
            int partitionIndex;
            PartitionKey partitionKey = consumerContext.getTablePartitionKey(tableName);
            StringBuilder[] dataBuffers = new StringBuilder[shardCount];
            for (int i = 0; i < shardCount; i++) {
                dataBuffers[i] = new StringBuilder();
            }
            StringBuilder localBuffer = new StringBuilder();
            String[] lines = event.getBatchLines();
            String partitionFieldValue;
            for (String line : lines) {
                if (StringUtils.isEmpty(line)) {
                    continue;
                }
                if (line == END_OF_BATCH_LINES) {
                    break;
                }
                String[] values = FileUtil.split(line, sep,
                    consumerContext.isWithLastSep(), hasEscapedQuote);
                partitionFieldValue = values[partitionKey.getFieldMetaInfo().getIndex()];
                partitionIndex = DbUtil.getPartitionIndex(partitionFieldValue, partitionKey);

                try {
                    fillLocalBuffer(localBuffer, values, fieldMetaInfoList);
                } catch (Throwable e) {
                    logger.error("{} at line: {}", e.getMessage(), line);
                    // 清空 继续处理下一行数据
                    localBuffer.setLength(0);
                    continue;
                }
                dataBuffers[partitionIndex].append(localBuffer);
                localBuffer.setLength(0);
            }
            for (int i = 0; i < shardCount; i++) {
                if (dataBuffers[i].length() != 0) {
                    execSqlWithShardingHint(topologyList.get(i), dataBuffers[i]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            // 认为无法恢复
            System.exit(1);
        } finally {
            consumerContext.getEmittedDataCounter().getAndDecrement();
            if (consumerContext.isUseBlock()) {
                consumerContext.getEventCounter().get(event.getLocalProcessingFileIndex()).
                    get(event.getLocalProcessingBlockIndex()).getAndDecrement();
            }
        }
    }

    /**
     * 根据切分出的字段值
     * 按照格式填充localBuffer
     */
    protected abstract void fillLocalBuffer(StringBuilder localBuffer, String[] values,
                                            List<FieldMetaInfo> fieldMetaInfoList) throws Throwable;

    /**
     * @param topology 表的分片逻辑
     * @param data 根据fillLocalBuffer得到的缓冲区数据
     */
    protected void execSqlWithShardingHint(TableTopology topology, StringBuilder data) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = consumerContext.getDataSource().getConnection();
            stmt = conn.createStatement();
            String sql = getSqlWithHint(topology, data);
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    /**
     * 根据实际操作类型(插入/更新/删除)来获取的sql语句
     *
     * @param topology 表的分片逻辑
     */
    protected abstract String getSqlWithHint(TableTopology topology, StringBuilder data);
}
