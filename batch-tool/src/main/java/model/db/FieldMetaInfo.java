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

import com.google.common.collect.ImmutableSet;

public class FieldMetaInfo {

    private String name;
    private Type type;
    /**
     * 从0开始
     */
    private int index;

    // todo 类型有待补充
    public static final ImmutableSet<String> STRING_TYPE_SET = ImmutableSet.of(
        "varchar",
        "char",
        "text"
    );

    public static final ImmutableSet<String> NUMBER_INT_TYPE_SET = ImmutableSet.of(
        "smallint",
        "integer",
        "int",
        "mediumint",
        "bigint"
        // ...
    );

    public static final ImmutableSet<String> NUMBER_FLOAT_TYPE_SET = ImmutableSet.of(
        "decimal",
        "float",
        "double"
    );

    public static final ImmutableSet<String> DATE_TYPE_SET = ImmutableSet.of(
        "date"
        //,"datetime"
        // ...
    );

    public enum Type {
        STRING,
        INT,
        FLOAT,
        DATE,
        OTHER
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public boolean needQuote() {
        switch (type) {
        case STRING:
        case DATE:
        case OTHER:
            return true;
        case INT:
        case FLOAT:
            return false;
        }
        return true;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setType(String typeStr) {
        if (STRING_TYPE_SET.contains(typeStr)) {
            this.type = Type.STRING;
        } else if (NUMBER_INT_TYPE_SET.contains(typeStr)) {
            this.type = Type.INT;
        } else if (NUMBER_FLOAT_TYPE_SET.contains(typeStr)) {
            this.type = Type.FLOAT;
        } else if (DATE_TYPE_SET.contains(typeStr)) {
            this.type = Type.DATE;
        } else {
            this.type = Type.OTHER;
        }
    }

    @Override
    public String toString() {
        return "FieldMetaInfo{" +
            "name='" + name + '\'' +
            ", type=" + type +
            ", index=" + index +
            '}';
    }
}
