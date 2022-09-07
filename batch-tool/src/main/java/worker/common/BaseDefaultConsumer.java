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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static model.config.ConfigConstant.END_OF_BATCH_LINES;

/**
 * 不进行分片处理的消费者
 */
public abstract class BaseDefaultConsumer extends BaseWorkHandler {
    private static final Logger logger = LoggerFactory.getLogger(BaseDefaultConsumer.class);

    protected int estimateFieldCount = 16;

    protected void initLocalVars() {
        super.initLocalVars();
    }

    @Override
    public void onProxyEvent(BatchLineEvent event) {
        if (consumerContext.getException() != null) {
            // fail fast on exception
            return;
        }
        initLocalVars();
        try {
            String[] lines = event.getBatchLines();
            int estimateLineSize = 10;
            if (lines.length > 0 && lines[0] != null) {
                estimateLineSize = Math.min(estimateLineSize, lines[0].length());
            }
            StringBuilder stringBuilder = new StringBuilder(lines.length * estimateLineSize);
            for (String line : lines) {
                if (StringUtils.isEmpty(line)) {
                    continue;
                }
                if (line == END_OF_BATCH_LINES) {
                    break;
                }
                List<String> values = FileUtil.splitWithEstimateCount(line, sep,
                    consumerContext.isWithLastSep(), estimateFieldCount, hasEscapedQuote);
                fillLocalBuffer(stringBuilder, values);
            }

            if (stringBuilder.length() > 0) {
                execSql(stringBuilder);
            }
        } catch (Exception e) {
            consumerContext.setException(e);
            logger.error("Failed in table [{}], due to {}", tableName, e.getMessage());
            // 认为无法恢复
            throw new RuntimeException(e);
        } finally {
            consumerContext.getEmittedDataCounter().getAndDecrement();
            if (consumerContext.isUseBlock()) {
                consumerContext.getEventCounter().get(event.getLocalProcessingFileIndex()).
                    get(event.getLocalProcessingBlockIndex()).getAndDecrement();
            }
        }
    }

    protected abstract void fillLocalBuffer(StringBuilder stringBuilder, List<String> values);

    protected abstract String getSql(StringBuilder data);

    protected void execSql(StringBuilder data) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        String sql = null;
        try {
            conn = consumerContext.getDataSource().getConnection();
            stmt = conn.createStatement();
            sql = getSql(data);
            stmt.execute(sql);
        } catch (SQLException e) {
//            logger.error(sql);
            throw e;
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }
}
