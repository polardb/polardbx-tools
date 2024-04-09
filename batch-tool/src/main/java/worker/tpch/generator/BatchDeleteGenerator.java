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
import model.config.GlobalVar;

public abstract class BatchDeleteGenerator extends BaseOrderLineUpdateGenerator {

    protected StringBuilder stringBuilder = new StringBuilder(128);

    public BatchDeleteGenerator(double scaleFactor, int round) {
        super(scaleFactor, round, GlobalVar.TPCH_UPDATE_DELETE_BATCH_NUM);
    }

    public abstract String getBatchDeleteKeys();

    public void next() {
        curIdx += batchSize;
    }

    @VisibleForTesting
    public abstract String getAllDeleteOrderkey();

    public void close() {
        this.stringBuilder = null;
    }

    /**
     * @return whether there is data in current batch
     */
    public boolean hasData() {
        return curIdx < count;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
