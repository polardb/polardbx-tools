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

public enum FileFormat {
    NONE("", true),
    TXT(".txt", true),
    CSV(".csv", true),
    LOG(".log", true),
    XLSX(".xlsx", false),
    ET(".et", false),
    XLS(".xls", false);

    private final String suffix;

    /**
     * 支持按定长块读写
     */
    private final boolean supportBlock;

    FileFormat(String suffix, boolean supportBlock) {
        this.suffix = suffix;
        this.supportBlock = supportBlock;
    }

    public static FileFormat fromString(String compressMode) {
        switch (compressMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "TXT":
            return TXT;
        case "CSV":
            return CSV;
        case "LOG":
            return LOG;
        case "XLSX":
            return XLSX;
        case "ET":
            return ET;
        case "XLS":
            return XLS;
        default:
            throw new IllegalArgumentException("Unrecognized file format: " + compressMode);
        }
    }

    public String getSuffix() {
        return suffix;
    }

    public boolean isSupportBlock() {
        return supportBlock;
    }

}
