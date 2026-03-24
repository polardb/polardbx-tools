/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package model.config;

public enum DdlMode {
    /**
     * 导出数据与DDL建表语句
     * 多个建表语句保存在同一个文件中
     */
    WITH_DDL,
    /**
     * 导出数据与DDL建表语句
     * 每张表对应一个文件
     */
    WITH_DDL_PER_TABLE,
    /**
     * 仅导出DDL建表语句
     */
    DDL_ONLY,
    /**
     * 仅导出DDL建表语句，每张表一个文件
     */
    DDL_ONLY_PER_TABLE,
    /**
     * 默认 不导出DDL建表语句
     */
    NO_DDL;

    public static DdlMode fromString(String ddlMode) {
        ddlMode = ddlMode.toUpperCase();
        // NONE / ONLY / WITH
        switch (ddlMode) {
        case "NONE":
            return NO_DDL;
        case "ONLY":
            return DDL_ONLY;
        case "ONLY_PER_TABLE":
        case "ONLY-PER-TABLE":
            return DDL_ONLY_PER_TABLE;
        case "WITH":
            return WITH_DDL;
        case "WITH_PER_TABLE":
        case "WITH-PER-TABLE":
            return WITH_DDL_PER_TABLE;
        default:
            throw new IllegalArgumentException("Illegal ddl mode: " + ddlMode);
        }
    }
}