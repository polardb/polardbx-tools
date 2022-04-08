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
import model.db.TableTopology;
import org.apache.commons.lang.StringUtils;
import util.FileUtil;
import worker.export.order.OrderByExportEvent;
import worker.export.order.ParallelOrderByExportEvent;

import java.util.Comparator;
import java.util.List;

import static model.config.ConfigConstant.ORDER_BY_TYPE_ASC;
import static model.config.ConfigConstant.ORDER_BY_TYPE_DESC;

public class ExportUtil {


    /**
     * 带日期格式转化
     */
    public static String getDirectSqlWithFormattedDate(TableTopology topology,
                                                       List<FieldMetaInfo> fieldMetaInfoList,
                                                       String whereCondition) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getDirectSqlWithFormattedDate(topology, fieldMetaInfoList);
        }

        return String.format("/*+TDDL:node='%s'*/ select %s from %s where %s;",
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), whereCondition);
    }

    private static String getDirectSqlWithFormattedDate(TableTopology topology,
                                                        List<FieldMetaInfo> fieldMetaInfoList) {

        return String.format("/*+TDDL:node='%s'*/ select %s from %s;",
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName());
    }

    /**
     * select 语句中的字段列表
     * 对日期类型进行 YYYYMMDD 的格式化
     * 如果后期还有其他类型格式 再重构
     */
    public static String formatFieldWithDateType(List<FieldMetaInfo> fieldMetaInfoList) {
        int fieldLen = fieldMetaInfoList.size();
        String[] toSelectFields = new String[fieldLen];
        FieldMetaInfo fieldMetaInfo;
        for (int i = 0; i < fieldLen; i++) {
            fieldMetaInfo = fieldMetaInfoList.get(i);
            if (fieldMetaInfo.getType() == FieldMetaInfo.Type.DATE) {
                // 对日期进行格式化
                toSelectFields[i] = String.format("DATE_FORMAT(%s, \"%%Y%%m%%d\")", fieldMetaInfo.getName());
            } else {
                toSelectFields[i] = fieldMetaInfo.getName();
            }
        }
        return StringUtils.join(toSelectFields, ",");
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       String columnName, boolean isAscending) {
        String orderType = isAscending ? "asc" : "desc";
        return String.format("/!TDDL:node='%s'*/ select %s from %s order by %s " + orderType,
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), columnName);
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       List<String> columnNameList, boolean isAscending) {
        String orderType = isAscending ? "asc" : "desc";
        return String.format("/!TDDL:node='%s'*/ select %s from %s order by %s " + orderType,
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(),
            StringUtils.join(columnNameList, ","));
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       String columnName,
                                       String whereCondition, boolean isAscending) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getOrderBySql(topology, fieldMetaInfoList, columnName, isAscending);
        }
        String orderType = isAscending ? "asc" : "desc";
        return String.format("/!TDDL:node='%s'*/ select %s from %s where %s order by %s " + orderType,
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), whereCondition, columnName);
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       List<String> columnNameList,
                                       String whereCondition, boolean isAscending) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getOrderBySql(topology, fieldMetaInfoList, columnNameList, isAscending);
        }
        String orderType = isAscending ? "asc" : "desc";
        return String.format("/!TDDL:node='%s'*/ select %s from %s where %s order by %s " + orderType,
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), whereCondition,
            StringUtils.join(columnNameList, ","));
    }

    public static String getDirectOrderBySql(String tableName,
                                             List<FieldMetaInfo> fieldMetaInfoList,
                                             List<String> columnNameList,
                                             String whereCondition, boolean isAscending) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getDirectOrderBySql(tableName, fieldMetaInfoList, columnNameList, isAscending);
        }
        String orderType = isAscending ? ORDER_BY_TYPE_ASC : ORDER_BY_TYPE_DESC;
        String columnNames = StringUtils.join(columnNameList, ",");
        return String
            .format("select %s from %s where %s order by %s " + orderType, formatFieldWithDateType(fieldMetaInfoList),
                tableName, whereCondition, columnNames);
    }

    public static String getDirectOrderBySql(String tableName,
                                             List<FieldMetaInfo> fieldMetaInfoList,
                                             List<String> columnNameList, boolean isAscending) {
        String orderType = isAscending ? ORDER_BY_TYPE_ASC : ORDER_BY_TYPE_DESC;
        String columnNames = StringUtils.join(columnNameList, ",");
        return String.format("select %s from %s order by %s " + orderType, formatFieldWithDateType(fieldMetaInfoList),
            tableName, columnNames);
    }

    public static Comparator<OrderByExportEvent> getCombinedComparator(List<FieldMetaInfo> orderByColumnInfoList) {
        Comparator<OrderByExportEvent> comparator = (o1, o2) -> {
            String val1, val2;
            int res;
            for (FieldMetaInfo fieldMetaInfo : orderByColumnInfoList) {
                val1 = new String(o1.getData()[fieldMetaInfo.getIndex()]);
                val2 = new String(o2.getData()[fieldMetaInfo.getIndex()]);
                // NULL值先默认最小
                if (val1.equals(FileUtil.NULL_ESC_STR)) {
                    if (val2.equals(FileUtil.NULL_ESC_STR)) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
                if (val2.equals(FileUtil.NULL_ESC_STR)) {
                    return 1;
                }
                switch (fieldMetaInfo.getType()) {
                case STRING:
                case OTHER:
                default:
                    res = val1.compareTo(val2);
                    break;
                case INT:
                    res = Integer.compare(Integer.parseInt(val1), Integer.parseInt(val2));
                    break;
                case FLOAT:
                    res = Double.compare(Double.parseDouble(val1), Double.parseDouble(val2));
                    break;
                }
                if (res != 0) {
                    return res;
                }
            }
            return 0;
        };
        return comparator;
    }

    public static Comparator<ParallelOrderByExportEvent> getCombinedParallelOrderComparator(
        List<FieldMetaInfo> orderByColumnInfoList) {
        Comparator<ParallelOrderByExportEvent> comparator = (o1, o2) -> {
            String val1, val2;
            int res;
            for (FieldMetaInfo fieldMetaInfo : orderByColumnInfoList) {
                val1 = new String(o1.getData()[fieldMetaInfo.getIndex()]);
                val2 = new String(o2.getData()[fieldMetaInfo.getIndex()]);
                // NULL值先默认最小
                if (val1.equals(FileUtil.NULL_ESC_STR)) {
                    if (val2.equals(FileUtil.NULL_ESC_STR)) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
                if (val2.equals(FileUtil.NULL_ESC_STR)) {
                    return 1;
                }
                switch (fieldMetaInfo.getType()) {
                case STRING:
                case OTHER:
                default:
                    res = val1.compareTo(val2);
                    break;
                case INT:
                    res = Integer.compare(Integer.parseInt(val1), Integer.parseInt(val2));
                    break;
                case FLOAT:
                    res = Double.compare(Double.parseDouble(val1), Double.parseDouble(val2));
                    break;
                }
                if (res != 0) {
                    return res;
                }
            }
            return 0;
        };
        return comparator;
    }
}
