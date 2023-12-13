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

public enum QuoteEncloseMode {
    /**
     * 根据字段（字符型）内是否含有特殊字符 如分隔符、换行符
     * 来添加双引号
     */
    AUTO,
    /**
     * 性能最差
     * 默认模式
     * 不管字段类型与内容，都添加双引号
     */
    FORCE,
    /**
     * 性能最好
     * 不管字段类型与内容，都不添加双引号
     * 如 已知表的字段都是数值型、或已知字段内容无特殊字符
     */
    NONE;

    public static QuoteEncloseMode parseMode(String mode) {
        switch (mode.toLowerCase()) {
        case "auto":
            return AUTO;
        case "force":
            return FORCE;
        case "none":
            return NONE;
        default:
            throw new IllegalArgumentException("Illegal mode: " + mode);
        }
    }
}
