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
 * 划分键
 */
public class PartitionKey {
    private FieldMetaInfo fieldMetaInfo;
    private int dbPartitionCount = 1;
    private int tbPartitionCount = 1;

    private int partitionSize = 0;

    public FieldMetaInfo getFieldMetaInfo() {
        return fieldMetaInfo;
    }

    public void setFieldMetaInfo(FieldMetaInfo fieldMetaInfo) {
        this.fieldMetaInfo = fieldMetaInfo;
    }

    public int getDbPartitionCount() {
        return dbPartitionCount;
    }

    public void setDbPartitionCount(int dbPartitionCount) {
        this.dbPartitionCount = dbPartitionCount;
    }

    public int getTbPartitionCount() {
        return tbPartitionCount;
    }

    public void setTbPartitionCount(int tbPartitionCount) {
        this.tbPartitionCount = tbPartitionCount;
    }

    public int getPartitionSize() {
        if (partitionSize == 0) {
            partitionSize = dbPartitionCount * tbPartitionCount;
        }
        return partitionSize;
    }

    public void setPartitionSize(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    @Override
    public String toString() {
        return "PartitionKey{" +
            "fieldMetaInfo=" + fieldMetaInfo +
            ", dbPartitionCount=" + dbPartitionCount +
            ", tbPartitionCount=" + tbPartitionCount +
            ", partitionSize=" + getPartitionSize() +
            '}';
    }
}
