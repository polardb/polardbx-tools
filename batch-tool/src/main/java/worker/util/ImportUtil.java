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

package worker.util;

import exception.DatabaseException;
import model.config.GlobalVar;
import model.db.FieldMetaInfo;
import org.apache.commons.lang.StringUtils;
import util.FileUtil;

import java.util.List;

import static worker.util.PolarxHint.DIRECT_NODE_HINT;

public class ImportUtil {

    private static final String BATCH_INSERT_SQL_PATTERN =
        "INSERT INTO `%s` VALUES %s;";

    private static final String BATCH_INSERT_IGNORE_SQL_PATTERN =
        "INSERT IGNORE INTO `%s` VALUES %s;";

    private static final String BATCH_INSERT_WITH_COL_SQL_PATTERN =
        "INSERT INTO `%s` (%s) VALUES %s;";

    private static final String BATCH_INSERT_IGNORE_SQL_WITH_COL_PATTERN =
        "INSERT IGNORE INTO `%s` (%s) VALUES %s;";

    private static final String BATCH_INSERT_HINT_SQL_PATTERN =
        DIRECT_NODE_HINT + "INSERT INTO `%s` VALUES %s;";

    private static final String BATCH_INSERT_IGNORE_HINT_SQL_PATTERN =
        DIRECT_NODE_HINT + "INSERT IGNORE INTO `%s` VALUES %s;";

    public static String getBatchInsertSql(String tableName, String values, boolean insertIgnoreEnabled) {
        if (insertIgnoreEnabled) {
            return String.format(BATCH_INSERT_IGNORE_SQL_PATTERN, tableName, values);
        } else {
            return String.format(BATCH_INSERT_SQL_PATTERN, tableName, values);
        }
    }

    public static void getBatchInsertSql(StringBuilder insertSqlBuilder,
                                         String tableName, String columns,
                                         StringBuilder values, boolean insertIgnoreEnabled) {
        insertSqlBuilder.append("INSERT ");
        if (insertIgnoreEnabled) {
            insertSqlBuilder.append("IGNORE ");
        }
        insertSqlBuilder.append("INTO `").append(tableName).append("` ");
        if (columns != null) {
            insertSqlBuilder.append('(').append(columns).append(") ");
        }
        insertSqlBuilder.append("VALUES ").append(values).append(";");
    }

    public static void appendInsertStrValue(StringBuilder sqlStringBuilder, String rawValue,
                                            boolean sqlEscapeEnabled, boolean hasEscapedQuote) {
        if (rawValue.equals(FileUtil.NULL_ESC_STR)) {
            // NULL字段处理
            sqlStringBuilder.append("NULL");
            return;
        }
        if (GlobalVar.IN_PERF_MODE) {
            sqlStringBuilder.append(rawValue);
            return;
        }
        if (sqlEscapeEnabled) {
            // 字符串要考虑转义
            sqlStringBuilder.append("'")
                .append(escapeSqlSpecialChar(rawValue))
                .append("'");
        } else {
            sqlStringBuilder.append("'").append(rawValue).append("'");
        }
    }

    /**
     * For MySQL
     * don't use StringEscapeUtils.escapeSql
     */
    private static String escapeSqlSpecialChar(String sqlValue) {
        if (!StringUtils.isEmpty(sqlValue)) {
            // 先处理反斜杠 \
            sqlValue = sqlValue.replaceAll("\\\\", "\\\\\\\\")
                .replaceAll("\b", "\\\\b")
                .replaceAll("\n", "\\\\n")
                .replaceAll("\r", "\\\\r")
                .replaceAll("\t", "\\\\t")
                .replaceAll("\\x1A", "\\\\Z")
                .replaceAll("\\x00", "\\\\0")
                .replaceAll("'", "\\\\'")
                .replaceAll("\"", "\\\\\"");
        }
        return sqlValue;
    }

    public static void appendInsertNonStrValue(StringBuilder sqlStringBuilder, String rawValue,
                                               boolean hasEscapedQuote) {
        if (rawValue.equals(FileUtil.NULL_ESC_STR)) {
            // NULL字段处理
            sqlStringBuilder.append("NULL");
        } else {
            sqlStringBuilder.append(rawValue);
        }
    }

    public static void appendValuesByFieldMetaInfo(StringBuilder stringBuilder,
                                                   List<FieldMetaInfo> fieldMetaInfoList,
                                                   String[] values, boolean sqlEscapeEnabled,
                                                   boolean hasEscapedQuote) throws DatabaseException {
        if (fieldMetaInfoList.size() != values.length) {
            throw new DatabaseException(String.format("required field size %d, "
                + "actual size %d", fieldMetaInfoList.size(), values.length));
        }
        int fieldLen = fieldMetaInfoList.size();
        for (int i = 0; i < fieldLen - 1; i++) {
            if (fieldMetaInfoList.get(i).needQuote()) {
                // 字符串和日期都需要单引号
                ImportUtil.appendInsertStrValue(stringBuilder, values[i], sqlEscapeEnabled, hasEscapedQuote);
            } else {
                ImportUtil.appendInsertNonStrValue(stringBuilder, values[i], hasEscapedQuote);
            }
            stringBuilder.append(",");
        }
        if (fieldMetaInfoList.get(fieldLen - 1).needQuote()) {
            ImportUtil.appendInsertStrValue(stringBuilder, values[fieldLen - 1], sqlEscapeEnabled, hasEscapedQuote);
        } else {
            ImportUtil.appendInsertNonStrValue(stringBuilder, values[fieldLen - 1], hasEscapedQuote);
        }
    }

    public static void getDirectImportSql(StringBuilder stringBuilder,
                                          String tableName,
                                          List<FieldMetaInfo> fieldMetaInfoList,
                                          String[] values, boolean sqlEscapeEnabled,
                                          boolean hasEscapedQuote) throws DatabaseException {
        stringBuilder.append("INSERT INTO `").append(tableName).append("` VALUES (");
        appendValuesByFieldMetaInfo(stringBuilder, fieldMetaInfoList, values,
            sqlEscapeEnabled, hasEscapedQuote);
        stringBuilder.append(");");
    }

    public static String getBatchInsertSqlWithHint(String nodeName, String tableName, String data,
                                                   boolean insertIgnoreEnabled) {
        if (insertIgnoreEnabled) {
            return String.format(BATCH_INSERT_IGNORE_HINT_SQL_PATTERN, nodeName, tableName, data);
        } else {
            return String.format(BATCH_INSERT_HINT_SQL_PATTERN, nodeName, tableName, data);
        }
    }

}
