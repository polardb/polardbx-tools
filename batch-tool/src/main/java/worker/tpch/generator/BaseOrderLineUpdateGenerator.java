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

package worker.tpch.generator;

import com.google.common.base.Preconditions;

/**
 * Delete first, then insert
 */
public class BaseOrderLineUpdateGenerator {

    /**
     * delete 10 rows in one sql
     */
    public static final int DEFAULT_DELETE_BATCH_NUM = 10;
    /**
     * insert 20 rows in one sql
     */
    public static final int DEFAULT_INSERT_BATCH_NUM = 20;

    public static final int SCALE_BASE = 1_500;
    public static final int MAX_UPDATE_ROUND = 100;

    protected final double scaleFactor;
    protected final long count;
    protected final long startIndex;
    protected final long endIndex;
    /**
     * the n-th round of update, starting from 1
     */
    protected final int round;
    protected int curIdx = 0;
    protected int batchSize;

    public BaseOrderLineUpdateGenerator(double scaleFactor, int round, int batchSize) {
        Preconditions.checkArgument(scaleFactor > 0,
            "Scale factor should be positive");
        Preconditions.checkArgument(round <= MAX_UPDATE_ROUND && round >= 1,
            "Update round should not be greater than " + MAX_UPDATE_ROUND);
        this.scaleFactor = scaleFactor;
        this.round = round;
        this.count = (long) (SCALE_BASE * scaleFactor);
        this.startIndex = (round - 1) * count;
        this.endIndex = round * count;
        this.batchSize = batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getRound() {
        return round;
    }

    public long getCount() {
        return count;
    }
}
