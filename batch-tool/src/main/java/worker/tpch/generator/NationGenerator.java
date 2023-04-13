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

import io.airlift.tpch.Distribution;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

/**
 * port from io.airlift.tpch
 */
public class NationGenerator extends TableRowGenerator {
    private static final int COMMENT_AVERAGE_LENGTH = 72;

    private final Distribution nations;
    private final RandomText commentRandom;

    public NationGenerator() {
        this(Distributions.getDefaultDistributions(),
            TextPool.getDefaultTestPool());
    }

    public NationGenerator(Distributions distributions, TextPool textPool) {
        super(distributions, textPool, 0, distributions.getNations().size());
        this.nations = distributions.getNations();
        this.commentRandom = new RandomText(606179079, textPool, COMMENT_AVERAGE_LENGTH);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        sqlBuffer.append('(').append(index)
            .append(",\"").append(nations.getValue((int) index))
            .append("\",").append(nations.getWeight((int) index))
            .append(",\"").append(commentRandom.nextValue()).append("\"),");

        commentRandom.rowFinished();
        index++;
    }
}
