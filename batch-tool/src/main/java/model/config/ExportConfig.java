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

package model.config;

import java.util.List;

/**
 * 导出的设置项
 */
public class ExportConfig extends BaseConfig {

    /**
     * 导出文件路径
     */
    private String path;

    /**
     * 导出文件名前缀
     */
    private String filenamePrefix;

    /**
     * 文件导出方式
     */
    private ExportWay exportWay;
    /**
     * 限制行数或文件数
     */
    private int limitNum;

    /**
     * where条件
     * 可选，若不填则为全表
     */
    private String whereCondition;

    /**
     * order by的字段名
     * 从0开始
     */
    private List<String> orderByColumnNameList;

    /**
     * 导出并发度，默认是分库分表并发级别
     */
    private int parallelism = 0;

    private boolean isAscending = true;
    private boolean isLocalMerge = false;
    private boolean isParallelMerge = false;

    public enum ExportWay {
        /**
         * 指定单个文件最大行数
         * 但不一定会到达最大值
         */
        MAX_LINE_NUM_IN_SINGLE_FILE,
        /**
         * 指定导出文件的总数量
         */
        FIXED_FILE_NUM,
        /**
         * 默认
         * 分库分表数即为文件数
         */
        DEFAULT
    }

    public ExportConfig() {
        this("", ConfigConstant.DEFAULT_SEPARATOR, ExportWay.DEFAULT,
            0, "", ConfigConstant.DEFAULT_QUOTE_ENCLOSE_MODE);
    }

    public ExportConfig(String filenamePrefix,
                        String separator,
                        ExportWay exportWay,
                        int limitNum,
                        String whereCondition,
                        QuoteEncloseMode quoteEncloseMode) {
        super(ConfigConstant.DEFAULT_EXPORT_SHARDING_ENABLED);
        this.separator = separator;
        this.filenamePrefix = filenamePrefix;
        this.exportWay = exportWay;
        this.limitNum = limitNum;
        this.whereCondition = whereCondition;
        this.quoteEncloseMode = quoteEncloseMode;
    }

    public void setFixedFileNum(int num) {
        this.exportWay = ExportWay.FIXED_FILE_NUM;
        this.limitNum = num;
    }

    public void setMaxLine(int num) {
        if (num < GlobalVar.EMIT_BATCH_SIZE) {
            throw new IllegalArgumentException("Max line should be greater than " + GlobalVar.EMIT_BATCH_SIZE);
        }
        this.exportWay = ExportWay.MAX_LINE_NUM_IN_SINGLE_FILE;
        this.limitNum = num;
    }

    public String getFilenamePrefix() {
        return filenamePrefix;
    }

    public void setFilenamePrefix(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    public ExportWay getExportWay() {
        return exportWay;
    }

    public void setExportWay(ExportWay exportWay) {
        this.exportWay = exportWay;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    public int getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(int limitNum) {
        this.limitNum = limitNum;
    }

    public List<String> getOrderByColumnNameList() {
        return orderByColumnNameList;
    }

    public void setOrderByColumnNameList(List<String> orderByColumnNameList) {
        this.orderByColumnNameList = orderByColumnNameList;
    }

    public String getPath() {
        if (path == null) {
            return "";
        }
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isAscending() {
        return isAscending;
    }

    public void setAscending(boolean ascending) {
        isAscending = ascending;
    }

    public boolean isLocalMerge() {
        return isLocalMerge;
    }

    public void setLocalMerge(boolean localMerge) {
        isLocalMerge = localMerge;
    }

    public boolean isParallelMerge() {
        return isParallelMerge;
    }

    public void setParallelMerge(boolean parallelMerge) {
        isParallelMerge = parallelMerge;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    private String getParallelismConfig() {
        if (this.parallelism > 0) {
            return String.valueOf(this.parallelism);
        } else {
            return "DEFAULT";
        }
    }

    @Override
    public String toString() {
        return "ExportConfig{" +
            "path='" + path + '\'' +
            ", filenamePrefix='" + filenamePrefix + '\'' +
            ", exportWay=" + exportWay +
            ", isWithHeader=" + isWithHeader() +
            ", limitNum=" + limitNum +
            ", whereCondition='" + whereCondition + '\'' +
            ", orderByColumnNameList=" + orderByColumnNameList +
            ", isAscending=" + isAscending +
            ", isLocalMerge=" + isLocalMerge +
            ", isParallelMerge=" + isParallelMerge +
            ", parallelism=" + getParallelismConfig() +
            "} " + super.toString();
    }
}
