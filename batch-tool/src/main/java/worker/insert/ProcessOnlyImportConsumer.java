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

package worker.insert;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.druid.util.StringUtils;
import exception.DatabaseException;
import model.db.FieldMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;
import worker.common.BaseWorkHandler;
import worker.common.BatchLineEvent;
import worker.util.ImportUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static model.config.ConfigConstant.END_OF_BATCH_LINES;

/**
 * 不执行sql语句
 * 用于测试读取文件生产者以及拼接语句消费者的效率
 */
public class ProcessOnlyImportConsumer extends BaseWorkHandler {
    private static final Logger logger = LoggerFactory.getLogger(ProcessOnlyImportConsumer.class);

    @Override
    public void onProxyEvent(BatchLineEvent event) {
        try {
            String[] lines = event.getBatchLines();

            List<FieldMetaInfo> fieldMetaInfoList = consumerContext.getTableFieldMetaInfo(tableName)
                .getFieldMetaInfoList();
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
                String[] values = FileUtil.split(line, consumerContext.getSeparator(),
                    consumerContext.isWithLastSep(), hasEscapedQuote);
                stringBuilder.append("(");
                try {
                    ImportUtil.appendValuesByFieldMetaInfo(stringBuilder, fieldMetaInfoList,
                        values, consumerContext.isSqlEscapeEnabled(), hasEscapedQuote);
                } catch (DatabaseException e) {
                    logger.error("Error {} at line: {}", e.getMessage(), line);
                    // 去除括号
                    stringBuilder.setLength(stringBuilder.length() - 1);
                    // 继续处理下一行数据
                    continue;
                }
                stringBuilder.append("),");
            }
            // 去除最后一个逗号 发送数据到数据库
            if (stringBuilder.length() > 0) {
                stringBuilder.setLength(stringBuilder.length() - 1);
            }
            insertData(stringBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void insertData(String data) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = consumerContext.getDataSource().getConnection();
            stmt = conn.createStatement();
            String sql = ImportUtil.getBatchInsertSql(tableName,
                data, consumerContext.isInsertIgnoreAndResumeEnabled());
            // 不执行
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
            consumerContext.getEmittedDataCounter().getAndDecrement();
        }
    }
}
