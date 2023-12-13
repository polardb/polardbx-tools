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

import org.apache.commons.lang3.StringUtils;

public class TableTopology {
    /**
     * 分库名
     */
    private final String groupName;
    /**
     * 分表名
     */
    private final String tableName;

    public TableTopology(String tableName) {
        this.tableName = tableName;
        this.groupName = null;
    }

    public TableTopology(String groupName, String tableName) {
        this.groupName = groupName;
        this.tableName = tableName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean hasGroup() {
        return StringUtils.isNotEmpty(groupName);
    }

    @Override
    public String toString() {
        if (groupName == null || groupName.length() == 0) {
            return "{tableName='" + tableName + "'}";
        }
        return "{" +
            "groupName='" + groupName + '\'' +
            ", tableName='" + tableName + '\'' +
            '}';
    }
}
