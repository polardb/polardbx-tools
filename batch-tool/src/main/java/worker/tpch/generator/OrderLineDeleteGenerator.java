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

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.NotThreadSafe;

import static worker.tpch.generator.OrderGenerator.ORDER_KEY_SPARSE_BITS;
import static worker.tpch.generator.OrderGenerator.ORDER_KEY_SPARSE_KEEP;

@NotThreadSafe
public class OrderLineDeleteGenerator extends BaseOrderLineUpdateGenerator {

    private StringBuilder stringBuilder = new StringBuilder(128);

    public OrderLineDeleteGenerator(double scaleFactor, int round) {
        super(scaleFactor, round, DEFAULT_DELETE_BATCH_NUM);
    }

    static long makeDeleteOrderKey(long orderIndex) {
        long lowBits = orderIndex & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

        long key = orderIndex;
        key >>= ORDER_KEY_SPARSE_KEEP;
        key <<= ORDER_KEY_SPARSE_BITS;
        key <<= ORDER_KEY_SPARSE_KEEP;
        key += lowBits;

        return key;
    }

    public String getBatchDeleteOrderkey() {
        stringBuilder.setLength(0);
        long start = startIndex + curIdx + 1;
        long remain = Math.min(count - start + 1, batchSize);
        if (remain <= 0) {
            throw new IllegalStateException("No more data in this batch");
        }
        for (long i = start; remain > 0; i++, remain--) {
            stringBuilder.append(makeDeleteOrderKey(i)).append(",");
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    public void next() {
        curIdx += batchSize;
    }

    @VisibleForTesting
    public String getAllDeleteOrderkey() {
        long left = count;
        for (long i = startIndex + 1; left > 0; i++, left--) {
            stringBuilder.append(makeDeleteOrderKey(i)).append(",");
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    /**
     * @return whether there is data in current batch
     */
    public boolean hasData() {
        return curIdx < count;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void close() {
        this.stringBuilder = null;
    }
}
