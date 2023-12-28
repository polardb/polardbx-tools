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

package model.stat;

import java.util.concurrent.TimeUnit;

public class SqlStat {

    private long totalTimeNanos = 0;
    private long count = 0;

    public synchronized void addTimeNs(long timeNanos) {
        totalTimeNanos += timeNanos;
        count++;
    }

    public synchronized double getAvgTimeMillis() {
        return count == 0 ? 0 : (double) TimeUnit.NANOSECONDS.toMillis(totalTimeNanos) / count;
    }

    public long getCount() {
        return count;
    }
}
