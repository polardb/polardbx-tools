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

import io.airlift.tpch.Distributions;
import io.airlift.tpch.TextPool;

public abstract class TableRowGenerator {

    protected final Distributions distributions;
    protected final TextPool textPool;
    protected final long startIndex;
    protected final long rowCount;

    protected long index = 0;

    public TableRowGenerator(Distributions distributions, TextPool textPool,
                             long startIndex, long rowCount) {
        this.distributions = distributions;
        this.textPool = textPool;
        this.startIndex = startIndex;
        this.rowCount = rowCount;
    }

    public boolean hasNext() {
        return index < rowCount;
    }

    /**
     * 以 sql values 的形式 append 一行
     * 尽可能不生成 String 中间对象
     */
    public abstract void appendNextRow(StringBuilder sqlBuffer);

}
