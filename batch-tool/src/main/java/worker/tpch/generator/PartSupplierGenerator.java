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

import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;

/**
 * port from io.airlift.tpch
 */
public class PartSupplierGenerator extends TableRowGenerator {
    private static final int SUPPLIERS_PER_PART = 4;

    private static final int AVAILABLE_QUANTITY_MIN = 1;
    private static final int AVAILABLE_QUANTITY_MAX = 9999;

    private static final int SUPPLY_COST_MIN = 100;
    private static final int SUPPLY_COST_MAX = 100000;

    private static final int COMMENT_AVERAGE_LENGTH = 124;

    private final double scaleFactor;

    public PartSupplierGenerator(double scaleFactor, int part, int partCount) {
        this(TextPool.getDefaultTestPool(),
            scaleFactor,
            calculateStartIndex(PartGenerator.SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(PartGenerator.SCALE_BASE, scaleFactor, part, partCount));
    }

    static long selectPartSupplier(long partKey, long supplierNumber, double scaleFactor) {
        long supplierCount = (long) (SupplierGenerator.SCALE_BASE * scaleFactor);
        return ((partKey + (supplierNumber * ((supplierCount / SUPPLIERS_PER_PART) + ((partKey - 1) / supplierCount))))
            % supplierCount) + 1;
    }

    private final RandomBoundedInt availableQuantityRandom;
    private final RandomBoundedInt supplyCostRandom;
    private final RandomText commentRandom;

    private long index;
    private int partSupplierNumber;

    public PartSupplierGenerator(TextPool textPool, double scaleFactor, long startIndex, long rowCount) {
        super(null, textPool, startIndex, rowCount);
        this.scaleFactor = scaleFactor;

        availableQuantityRandom =
            new RandomBoundedInt(1671059989, AVAILABLE_QUANTITY_MIN, AVAILABLE_QUANTITY_MAX, SUPPLIERS_PER_PART);
        supplyCostRandom = new RandomBoundedInt(1051288424, SUPPLY_COST_MIN, SUPPLY_COST_MAX, SUPPLIERS_PER_PART);
        commentRandom = new RandomText(1961692154, textPool, COMMENT_AVERAGE_LENGTH, SUPPLIERS_PER_PART);

        availableQuantityRandom.advanceRows(startIndex);
        supplyCostRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        long partKey = startIndex + index + 1;

        sqlBuffer.append('(').append(partKey)
            .append(',').append(selectPartSupplier(partKey, partSupplierNumber, scaleFactor))
            .append(',').append(availableQuantityRandom.nextValue())
            .append(',');

        appendDecimalWithFrac2(sqlBuffer, supplyCostRandom.nextValue());

        sqlBuffer.append(",\"").append(commentRandom.nextValue()).append("\"),");

        partSupplierNumber++;

        // advance next row only when all lines for the order have been produced
        if (partSupplierNumber >= SUPPLIERS_PER_PART) {
            availableQuantityRandom.rowFinished();
            supplyCostRandom.rowFinished();
            commentRandom.rowFinished();

            index++;
            partSupplierNumber = 0;
        }
    }
}
