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

package model.db;

/**
 * todo 与FieldMetaInfo重构
 */
public class PrimaryKey {
    /**
     * ordinalPosition从1开始
     */
    private int ordinalPosition;
    private String name;

    public PrimaryKey(int ordinalPosition, String name) {
        this.ordinalPosition = ordinalPosition;
        this.name = name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "PrimaryKey{" +
            "ordinalPosition=" + ordinalPosition +
            ", name='" + name + '\'' +
            '}';
    }
}
