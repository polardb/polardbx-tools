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
     */
    WITH_DDL,
    /**
     * 仅导出DDL建表语句
     */
    DDL_ONLY,
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
        case "WITH":
            return WITH_DDL;
        default:
            throw new IllegalArgumentException("Illegal ddl mode: " + ddlMode);
        }
    }
}