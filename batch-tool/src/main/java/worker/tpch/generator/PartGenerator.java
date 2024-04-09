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

import io.airlift.tpch.Distributions;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomStringSequence;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;

/**
 * port from io.airlift.tpch
 */
public class PartGenerator extends TableRowGenerator {
    public static final int SCALE_BASE = 200_000;

    private static final int NAME_WORDS = 5;
    private static final int MANUFACTURER_MIN = 1;
    private static final int MANUFACTURER_MAX = 5;
    private static final int BRAND_MIN = 1;
    private static final int BRAND_MAX = 5;
    private static final int SIZE_MIN = 1;
    private static final int SIZE_MAX = 50;
    private static final int COMMENT_AVERAGE_LENGTH = 14;

    private final RandomStringSequence nameRandom;
    private final RandomBoundedInt manufacturerRandom;
    private final RandomBoundedInt brandRandom;
    private final RandomString typeRandom;
    private final RandomBoundedInt sizeRandom;
    private final RandomString containerRandom;
    private final RandomText commentRandom;

    public PartGenerator(double scaleFactor, int part, int partCount) {
        this(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool(),
            calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    public PartGenerator(Distributions distributions, TextPool textPool, long startIndex, long rowCount) {
        super(distributions, textPool, startIndex, rowCount);

        nameRandom = new RandomStringSequence(709314158, NAME_WORDS, distributions.getPartColors());
        manufacturerRandom = new RandomBoundedInt(1, MANUFACTURER_MIN, MANUFACTURER_MAX);
        brandRandom = new RandomBoundedInt(46831694, BRAND_MIN, BRAND_MAX);
        typeRandom = new RandomString(1841581359, distributions.getPartTypes());
        sizeRandom = new RandomBoundedInt(1193163244, SIZE_MIN, SIZE_MAX);
        containerRandom = new RandomString(727633698, distributions.getPartContainers());
        commentRandom = new RandomText(804159733, textPool, COMMENT_AVERAGE_LENGTH);

        nameRandom.advanceRows(startIndex);
        manufacturerRandom.advanceRows(startIndex);
        brandRandom.advanceRows(startIndex);
        typeRandom.advanceRows(startIndex);
        sizeRandom.advanceRows(startIndex);
        containerRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        long partKey = startIndex + index + 1;

        int manufacturer = manufacturerRandom.nextValue();
        int brand = manufacturer * 10 + brandRandom.nextValue();

        sqlBuffer.append('(').append(partKey)
            .append(",\"").append(nameRandom.nextValue())
            .append("\",\"Manufacturer#").append(manufacturer)
            .append("\",\"Brand#").append(brand)
            .append("\",\"").append(typeRandom.nextValue())
            .append("\",").append(sizeRandom.nextValue())
            .append(",\"").append(containerRandom.nextValue())
            .append("\",");

        appendDecimalWithFrac2(sqlBuffer, calculatePartPrice(partKey));

        sqlBuffer.append(",\"").append(commentRandom.nextValue()).append("\"),");

        nameRandom.rowFinished();
        manufacturerRandom.rowFinished();
        brandRandom.rowFinished();
        typeRandom.rowFinished();
        sizeRandom.rowFinished();
        containerRandom.rowFinished();
        commentRandom.rowFinished();

        index++;
    }

    static long calculatePartPrice(long p) {
        long price = 90000;

        // limit contribution to $200
        price += (p / 10) % 20001;
        price += (p % 1000) * 100;

        return (price);
    }
}
