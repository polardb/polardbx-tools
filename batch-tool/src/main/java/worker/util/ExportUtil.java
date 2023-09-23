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

import model.config.CompressMode;
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
import static worker.util.PolarxHint.DIRECT_NODE_HINT;

public class ExportUtil {

    public static String getDirectSql(TableTopology topology,
                                      List<FieldMetaInfo> fieldMetaInfoList,
                                      String whereCondition) {
        if (StringUtils.isEmpty(whereCondition)) {
            return getDirectSql(topology, fieldMetaInfoList);
        }

        return String.format(DIRECT_NODE_HINT + "select %s from %s where %s;",
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), whereCondition);
    }

    private static String getDirectSql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList) {
        if (topology.getGroupName().isEmpty()) {
            return String.format("select %s from %s;", formatFieldWithDateType(fieldMetaInfoList),
                topology.getTableName());
        }
        return String.format(DIRECT_NODE_HINT + "select %s from %s;",
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName());
    }

    /**
     * select 语句中的字段列表
     */
    public static String formatFieldWithDateType(List<FieldMetaInfo> fieldMetaInfoList) {
        if (fieldMetaInfoList.isEmpty()) {
            throw new IllegalArgumentException("Empty field meta info");
        }
        int fieldLen = fieldMetaInfoList.size();
        FieldMetaInfo fieldMetaInfo;
        StringBuilder stringBuilder = new StringBuilder(fieldLen * 8);
        for (FieldMetaInfo metaInfo : fieldMetaInfoList) {
            fieldMetaInfo = metaInfo;
            /*
                后期再支持如时间日期字段值的格式化
             */
//            if (fieldMetaInfo.getType() == FieldMetaInfo.Type.DATE) {
//                // 对日期进行格式化
//                stringBuilder.append("DATE_FORMAT(`").append(fieldMetaInfo.getName())
//                    .append("`, \"%%Y%%m%%d\")");
//            }
            stringBuilder.append('`').append(fieldMetaInfo.getName()).append('`');
            stringBuilder.append(',');
        }
        if (stringBuilder.length() > 0) {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       String columnName, boolean isAscending) {
        String orderType = isAscending ? "asc" : "desc";
        return String.format(DIRECT_NODE_HINT + "select %s from %s order by %s " + orderType,
            topology.getGroupName(), formatFieldWithDateType(fieldMetaInfoList),
            topology.getTableName(), columnName);
    }

    public static String getOrderBySql(TableTopology topology,
                                       List<FieldMetaInfo> fieldMetaInfoList,
                                       List<String> columnNameList, boolean isAscending) {
        String orderType = isAscending ? "asc" : "desc";
        return String.format(DIRECT_NODE_HINT + "select %s from %s order by %s " + orderType,
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
        return String.format(DIRECT_NODE_HINT + "select %s from %s where %s order by %s " + orderType,
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
        return String.format(DIRECT_NODE_HINT + "select %s from %s where %s order by %s " + orderType,
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

    public static String getFilename(String filename, CompressMode compressMode) {
        if (compressMode == CompressMode.GZIP) {
            return filename + ".gz";
        }
        return filename;
    }
}
