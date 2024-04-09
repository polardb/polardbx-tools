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

package worker.tpch.model;

public enum TpchTableModel {

    LINEITEM("lineitem", 16, 160),
    CUSTOMER("customer", 8, 176),
    ORDERS("orders", 9, 128),
    PART("part", 9, 128),
    SUPPLIER("supplier", 7, 128),
    PART_SUPP("partsupp", 5, 150),
    NATION("nation", 4, 90),
    REGION("region", 3, 135);

    private final String name;
    private final int fieldCount;

    /**
     * 预计算好的行长度
     * 包含：分隔符、引号
     */
    private final int rowStrLen;

    TpchTableModel(String name, int fieldCount, int rowStrLen) {
        this.name = name;
        this.fieldCount = fieldCount;
        this.rowStrLen = rowStrLen;
    }

    public static TpchTableModel parse(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Empty table name");
        }
        name = name.toLowerCase();
        for (TpchTableModel value : TpchTableModel.values()) {
            if (value.name.equals(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported table name: " + name);
    }

    public String getName() {
        return name;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public int getRowStrLen() {
        return rowStrLen;
    }
}
