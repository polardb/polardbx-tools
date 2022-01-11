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

import model.db.FieldMetaInfo;
import model.db.PrimaryKey;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import util.DbUtil;

import java.util.List;

public class DeleteUtil {

    private static final String BATCH_DELETE_HINT_SQL_PATTERN =
        "/!TDDL:node='%s'*/ DELETE FROM `%s` WHERE (%s) IN (%s) ";

    private static final String BATCH_DELETE_WHERE_HINT_SQL_PATTERN =
        "/!TDDL:node='%s'*/ DELETE FROM `%s` WHERE (%s) IN (%s) "
            + "AND %s";

    /**
     * 批量删除Sql语句
     *
     * @param tableName 表名
     * @param pkList 主键
     * @param values 待删除的值，带单引号、以逗号分隔
     */
    public static String getBatchDeleteSql(String tableName, List<PrimaryKey> pkList, String values) {
        String sqlPattern = "DELETE FROM `%s` WHERE (%s) IN (%s);";
        String pkSet = formatPkList(pkList);
        return String.format(sqlPattern, tableName, pkSet, values);
    }

    /**
     * 批量删除Sql语句
     *
     * @param tableName 表名
     * @param pkList 主键列表
     * @param values 待删除的值，带单引号、以逗号分隔
     * @param where 附加的where条件
     */
    public static String getBatchDeleteSql(String tableName, List<PrimaryKey> pkList, String values, String where) {
        if (StringUtils.isEmpty(where)) {
            return getBatchDeleteSql(tableName, pkList, values);
        }
        String sqlPattern = "DELETE FROM `%s` WHERE (%s) IN (%s) AND %s;";
        String pkSet = formatPkList(pkList);
        return String.format(sqlPattern, tableName, pkSet, values, where);
    }

    /**
     * hash 根据主键delete一行
     */
    public static String getDeleteSqlWithHint(String nodeName, String tableName, List<PrimaryKey> pkList,
                                              String[] values, String where) {
        if (StringUtils.isEmpty(where)) {
            return getDeleteSqlWithHint(nodeName, tableName, pkList, values);
        }
        String sqlPattern = "/!TDDL:node='%s'*/ DELETE FROM `%s` WHERE %s AND %s;";
        String[] pkConditions = new String[pkList.size()];
        for (int i = 0; i < pkList.size(); i++) {
            pkConditions[i] = pkList.get(i).getName() + "='" + values[i] + "'";
        }
        String pkCondition = StringUtils.join(pkConditions, " AND ");
        return String.format(sqlPattern, nodeName, tableName, pkCondition, where);
    }

    /**
     * hash 根据主键delete一行
     */
    public static String getDeleteSqlWithHint(String nodeName, String tableName, List<PrimaryKey> pkList,
                                              String[] values) {
        String sqlPattern = "/!TDDL:node='%s'*/ DELETE FROM `%s` WHERE %s ;";
        String[] pkConditions = new String[pkList.size()];
        for (int i = 0; i < pkList.size(); i++) {
            pkConditions[i] = pkList.get(i).getName() + "='" + values[i] + "'";
        }
        String pkCondition = StringUtils.join(pkConditions, " AND ");
        return String.format(sqlPattern, nodeName, tableName, pkCondition);
    }

    /**
     * 简单的根据主键delete一行
     */
    public static String getDeleteSql(String tableName, List<PrimaryKey> pkList, String[] values, String where) {
        if (StringUtils.isEmpty(where)) {
            return getDeleteSql(tableName, pkList, values);
        }
        String sqlPattern = "DELETE FROM `%s` WHERE %s AND %s;";
        String pkCondition = DbUtil.formatPkConditions(pkList, values);
        return String.format(sqlPattern, tableName, pkCondition, where);
    }

    public static String getDeleteSql(String tableName, List<PrimaryKey> pkList, String[] values) {
        String sqlPattern = "DELETE FROM `%s` WHERE %s;";
        String pkCondition = DbUtil.formatPkConditions(pkList, values);
        return String.format(sqlPattern, tableName, pkCondition);
    }

    public static String getBatchDeleteSqlWithHint(String nodeName, String tableName,
                                                   List<PrimaryKey> pkList, String data,
                                                   String where) {
        String pkSet = formatPkList(pkList);
        if (StringUtils.isEmpty(where)) {
            return String.format(BATCH_DELETE_HINT_SQL_PATTERN,
                nodeName, tableName, pkSet, data);
        } else {
            return String.format(BATCH_DELETE_WHERE_HINT_SQL_PATTERN,
                nodeName, tableName, pkSet, data, where);
        }
    }

    public static String formatPkList(List<PrimaryKey> pkList) {
        String[] pkNames = new String[pkList.size()];
        for (int i = 0; i < pkList.size(); i++) {
            pkNames[i] = pkList.get(i).getName();
        }
        return StringUtils.join(pkNames, ",");
    }

    public static void appendPkValuesByFieldMetaInfo(StringBuilder localBuffer, List<FieldMetaInfo> fieldMetaInfoList,
                                                     List<PrimaryKey> pkList, String[] pkValues) {
        // 主键不为NULL 不需要考虑NULL字段
        int i;
        for (i = 0; i < pkList.size(); i++) {
            int idx = pkList.get(i).getOrdinalPosition() - 1;
            if (fieldMetaInfoList.get(idx).getType() == FieldMetaInfo.Type.STRING ||
                fieldMetaInfoList.get(idx).getType() == FieldMetaInfo.Type.OTHER) {
                // 字符串和日期都需要单引号
                localBuffer.append("'")
                    .append(StringEscapeUtils.escapeSql(pkValues[i]))
                    .append("'");
            } else {
                localBuffer.append(pkValues[i]);
            }
            localBuffer.append(",");
        }
        if (i > 0) {
            // 去除逗号
            localBuffer.setLength(localBuffer.length() - 1);
        }
    }

    public static String getDeleteUsingIn(String tableName, String pkNames, String inPkValues) {
        String deleteSqlPattern = "delete from %s where (%s) in (%s)";
        return String.format(deleteSqlPattern, tableName, pkNames, inPkValues);
    }
}
