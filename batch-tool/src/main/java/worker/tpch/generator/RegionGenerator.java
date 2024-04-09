/*
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
public class RegionGenerator extends TableRowGenerator {
    private static final int COMMENT_AVERAGE_LENGTH = 72;

    private final Distribution regions;
    private final RandomText commentRandom;

    public RegionGenerator() {
        this(Distributions.getDefaultDistributions(),
            TextPool.getDefaultTestPool());
    }

    public RegionGenerator(Distributions distributions, TextPool textPool) {
        super(distributions, textPool, 0, distributions.getRegions().size());

        this.regions = distributions.getRegions();
        this.commentRandom = new RandomText(1500869201, textPool, COMMENT_AVERAGE_LENGTH);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        sqlBuffer.append('(').append(index)
            .append(",\"").append(regions.getValue((int) index))
            .append("\",\"").append(commentRandom.nextValue()).append("\"),");

        commentRandom.rowFinished();
        index++;
    }
}
