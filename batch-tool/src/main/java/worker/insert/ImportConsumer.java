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

import exception.DatabaseException;
import model.config.ConfigConstant;
import model.db.FieldMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BaseDefaultConsumer;
import worker.util.ImportUtil;

import java.util.List;

public class ImportConsumer extends BaseDefaultConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ImportConsumer.class);

    private List<FieldMetaInfo> fieldMetaInfoList;
    /**
     * 仅指定了列名才会设置
     */
    private String columns = null;
    private StringBuilder insertSqlBuilder;

    @Override
    protected void initLocalVars() {
        super.initLocalVars();
        this.fieldMetaInfoList = consumerContext.getTableFieldMetaInfo(tableName).getFieldMetaInfoList();
        this.estimateFieldCount = fieldMetaInfoList.size();
        this.columns = consumerContext.getUseColumns();
        this.insertSqlBuilder = new StringBuilder(64 + fieldMetaInfoList.size() * 16);
    }

    @Override
    protected void fillLocalBuffer(StringBuilder stringBuilder, List<String> values) {
        stringBuilder.append("(");
        try {
            ImportUtil.appendValuesByFieldMetaInfo(stringBuilder, fieldMetaInfoList,
                values, consumerContext.isSqlEscapeEnabled(), hasEscapedQuote);
        } catch (DatabaseException e) {
            // 在split预处理过后仍存在的问题
            logger.error(StringUtils.join(values, ConfigConstant.MAGIC_CSV_SEP));
            throw new RuntimeException(e);
        }

        stringBuilder.append("),");
    }

    @Override
    protected String getSql(StringBuilder data) {
        // 去除最后一个逗号
        data.setLength(data.length() - 1);
        ImportUtil.getBatchInsertSql(insertSqlBuilder, tableName, columns,
            data, consumerContext.isInsertIgnoreAndResumeEnabled());
        String sql = insertSqlBuilder.toString();
        insertSqlBuilder.setLength(0);
        return sql;
    }
}
