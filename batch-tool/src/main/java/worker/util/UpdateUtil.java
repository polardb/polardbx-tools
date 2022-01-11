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
import model.db.TableFieldMetaInfo;
import org.apache.commons.lang.StringUtils;
import util.DbUtil;
import util.FileUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static model.config.ConfigConstant.FLOAT_UPDATE_MULTIPLICAND;
import static model.config.ConfigConstant.INT_UPDATE_MULTIPLICAND;

public class UpdateUtil {

    private static final String BATCH_REPLACE_PATTERN_WITH_HINT = "/!TDDL:node='%s'*/ REPLACE INTO `%s`(%s) "
        + "VALUES %s;";

    private static final String BATCH_REPLACE_PATTERN = " REPLACE INTO `%s`(%s) "
        + "VALUES %s;";

    public static String getPreparedUpdateSql(String tableName, TableFieldMetaInfo tableFieldMetaInfo) {
        String sqlPattern = "UPDATE `%s` SET %s WHERE %s;";
        String wherePk = tableFieldMetaInfo.getPrimaryKey().getName() + "=?";
        String update = formatTableFieldWithPlaceholder(tableFieldMetaInfo);
        return String.format(sqlPattern, tableName, update, wherePk);
    }

    public static String getPreparedUpdateSql(String tableName,
                                              TableFieldMetaInfo tableFieldMetaInfo,
                                              String whereCondition) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getPreparedUpdateSql(tableName, tableFieldMetaInfo);
        }
        String sqlPattern = "UPDATE `%s` SET %s WHERE %s AND %s;";
        String wherePk = tableFieldMetaInfo.getPrimaryKey().getName() + "=?";
        String update = formatTableFieldWithPlaceholder(tableFieldMetaInfo);
        return String.format(sqlPattern, tableName, update, wherePk, whereCondition);
    }

    /**
     * @return col1, col2, col3 ...
     */
    public static String formatToReplaceColumns(TableFieldMetaInfo tableFieldMetaInfo) {
        List<FieldMetaInfo> fieldMetaInfoList = tableFieldMetaInfo.getFieldMetaInfoList();
        StringBuilder stringBuilder = new StringBuilder(fieldMetaInfoList.size() * 8);
        for (int i = 0; i < fieldMetaInfoList.size() - 1; i++) {
            stringBuilder.append(fieldMetaInfoList.get(i).getName()).append(",");
        }
        stringBuilder.append(fieldMetaInfoList.get(fieldMetaInfoList.size() - 1).getName());
        return stringBuilder.toString();
    }

    public static String getBatchReplaceSqlWithHint(String nodeName,
                                                    String tableName,
                                                    String toReplaceColumns,
                                                    String data) {
        return String.format(BATCH_REPLACE_PATTERN_WITH_HINT, nodeName, tableName,
            toReplaceColumns, data);
    }

    public static String getBatchReplaceSql(String tableName,
                                            String toReplaceColumns,
                                            String data) {
        return String.format(BATCH_REPLACE_PATTERN, tableName,
            toReplaceColumns, data);
    }

    /**
     * 根据字段类型对值进行更新
     */
    public static String getUpdatedValuesByMetaInfo(Set<Integer> pkIndexSet, String[] values,
                                                    List<FieldMetaInfo> fieldMetaInfoList) {
        List<String> updatedValueList = new ArrayList<>(fieldMetaInfoList.size());
        String fieldValue;
        int fieldIntValue;
        float fieldFloatValue;
        for (FieldMetaInfo fieldMetaInfo : fieldMetaInfoList) {
            fieldValue = values[fieldMetaInfo.getIndex()];
            if (pkIndexSet.contains(fieldMetaInfo.getIndex())) {
                // 主键不变
                if (fieldMetaInfo.getType() == FieldMetaInfo.Type.STRING) {
                    updatedValueList.add("'" + fieldValue + "'");
                } else {
                    updatedValueList.add(fieldValue);
                }
                continue;
            }
            if (fieldValue.equals(FileUtil.NULL_ESC_STR)) {
                // NULL值不变
                updatedValueList.add(FileUtil.NULL_STR);
                continue;
            }
            // 更新数据
            switch (fieldMetaInfo.getType()) {
            case STRING:
                // 反转字符串
                updatedValueList.add("'" + StringUtils.reverse(fieldValue) + "'");
                break;
            case INT:
                // 整型直接乘2 不考虑溢出
                fieldIntValue = Integer.parseInt(fieldValue);
                updatedValueList.add(String.valueOf(fieldIntValue * INT_UPDATE_MULTIPLICAND));
                break;
            case FLOAT:
                // 使用float 不用BigDecimal
                fieldFloatValue = Float.parseFloat(fieldValue);
                updatedValueList.add(String.valueOf((fieldFloatValue * FLOAT_UPDATE_MULTIPLICAND)));
                break;
            default:
                updatedValueList.add("'" + fieldValue + "'");
                break;
            }
        }

        return StringUtils.join(updatedValueList, ",");
    }

    /**
     * 如 col1=?,col2=?,col3=? ...
     */
    private static String formatTableFieldWithPlaceholder(TableFieldMetaInfo tableFieldMetaInfo) {
        int fieldLen = tableFieldMetaInfo.getFieldMetaInfoList().size();
        int pkIndex = tableFieldMetaInfo.getPrimaryKey().getOrdinalPosition() - 1;
        String[] updateFields = new String[fieldLen - 1];
        for (int i = 0, j = 0; i < fieldLen; i++, j++) {
            if (i != pkIndex) {
                updateFields[j] = tableFieldMetaInfo.getFieldMetaInfoList().get(i).getName() + "=?";
            } else {
                j--;
            }
        }
        return StringUtils.join(updateFields, ",");
    }

    public static String getUpdateSql(String tableName, List<PrimaryKey> pkList, Set<Integer> pkIndexSet,
                                      List<FieldMetaInfo> fieldMetaInfoList, String[] values,
                                      String where) {
        if (StringUtils.isEmpty(where)) {
            return getUpdateSql(tableName, pkList,
                pkIndexSet, fieldMetaInfoList, values);
        }
        String updateSqlPattern = "UPDATE %s SET %s WHERE %s AND %s;";
        String pkCondition = DbUtil.formatPkConditions(pkList, values);
        String setUpdatedValue = formatSetUpdatedValues(pkIndexSet, fieldMetaInfoList, values);

        return String.format(updateSqlPattern, tableName, setUpdatedValue, pkCondition, where);
    }

    public static String getUpdateSql(String tableName, List<PrimaryKey> pkList, Set<Integer> pkIndexSet,
                                      List<FieldMetaInfo> fieldMetaInfoList, String[] values) {
        String updateSqlPattern = "UPDATE %s SET %s WHERE %s;";
        String pkCondition = DbUtil.formatPkConditions(pkList, values);
        String setUpdatedValue = formatSetUpdatedValues(pkIndexSet, fieldMetaInfoList, values);

        return String.format(updateSqlPattern, tableName, setUpdatedValue, pkCondition);
    }

    public static String formatSetUpdatedValues(Set<Integer> pkIndexSet,
                                                List<FieldMetaInfo> fieldMetaInfoList,
                                                String[] values) {
        List<String> updatedValueList = new ArrayList<>(fieldMetaInfoList.size() - pkIndexSet.size());
        String fieldValue;
        int fieldIntValue;
        float fieldFloatValue;
        for (FieldMetaInfo fieldMetaInfo : fieldMetaInfoList) {
            fieldValue = values[fieldMetaInfo.getIndex()];
            if (pkIndexSet.contains(fieldMetaInfo.getIndex())) {
                // 主键不在set的值里面
                continue;
            }
            if (fieldValue.equals(FileUtil.NULL_ESC_STR)) {
                // NULL值不变
                updatedValueList.add(fieldMetaInfo.getName() + "=" + FileUtil.NULL_STR);
                continue;
            }
            // 更新数据
            switch (fieldMetaInfo.getType()) {
            case STRING:
                // 反转字符串
                updatedValueList.add(fieldMetaInfo.getName() + "='"
                    + StringUtils.reverse(fieldValue) + "'");
                break;
            case INT:
                // 整型直接乘2 不考虑溢出
                fieldIntValue = Integer.parseInt(fieldValue);
                updatedValueList.add(fieldMetaInfo.getName() + "=" +
                    fieldIntValue * INT_UPDATE_MULTIPLICAND);
                break;
            case FLOAT:
                // 使用float 不用BigDecimal
                fieldFloatValue = Float.parseFloat(fieldValue);
                updatedValueList.add(fieldMetaInfo.getName() + "=" +
                    fieldFloatValue * FLOAT_UPDATE_MULTIPLICAND);
                break;
            default:
                // 默认不变
                break;
            }
        }
        return StringUtils.join(updatedValueList, ",");
    }

    public static String getUpdateWithFuncSql(String updateWithFuncPattern, List<PrimaryKey> pkList,
                                              String[] pkValues) {
        String pkCondition = DbUtil.formatPkConditions(pkList, pkValues);
        return String.format(updateWithFuncPattern, pkCondition);
    }

    public static String getUpdateWithFuncInSql(String updateWithFuncPattern, String pkNames,
                                                String inPkValues) {

        String pkInCondition = String.format("(%s) in (%s)", pkNames, inPkValues);
        return String.format(updateWithFuncPattern, pkInCondition);
    }

    public static String getUpdateWithFuncSqlPattern(String tableName,
                                                     List<FieldMetaInfo> fieldMetaInfoList,
                                                     Set<Integer> pkIndexSet) {
        String basePattern = "UPDATE %s SET %s WHERE";
        String updateWithFunc = getUpdateFunction(fieldMetaInfoList, pkIndexSet);
        String formattedPattern = String.format(basePattern, tableName, updateWithFunc);

        return formattedPattern + " %s;";
    }

    private static String getUpdateFunction(List<FieldMetaInfo> fieldMetaInfoList,
                                            Set<Integer> pkIndexSet) {
        List<String> updatedValueList = new ArrayList<>(fieldMetaInfoList.size() - pkIndexSet.size());
        for (FieldMetaInfo fieldMetaInfo : fieldMetaInfoList) {
            if (pkIndexSet.contains(fieldMetaInfo.getIndex())) {
                // 主键不在set的值里面
                continue;
            }
            // 更新数据
            switch (fieldMetaInfo.getType()) {
            case STRING:
                // 反转字符串
                updatedValueList.add(fieldMetaInfo.getName() + "=REVERSE("
                    + fieldMetaInfo.getName() + ")");
                break;
            case INT:
            case FLOAT:
                updatedValueList.add(fieldMetaInfo.getName() + "=2*"
                    + fieldMetaInfo.getName());
                break;
            default:
                // 默认不变
                break;
            }
        }
        return StringUtils.join(updatedValueList, ",");
    }
}
