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

import io.airlift.tpch.CustomerGenerator;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomBoundedLong;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import static io.airlift.tpch.GenerateUtils.GENERATED_DATE_EPOCH_OFFSET;
import static io.airlift.tpch.GenerateUtils.MIN_GENERATE_DATE;
import static io.airlift.tpch.GenerateUtils.TOTAL_DATE_RANGE;
import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static worker.tpch.generator.LineItemGenerator.ITEM_SHIP_DAYS;
import static worker.tpch.generator.LineItemGenerator.createDiscountRandom;
import static worker.tpch.generator.LineItemGenerator.createPartKeyRandom;
import static worker.tpch.generator.LineItemGenerator.createQuantityRandom;
import static worker.tpch.generator.LineItemGenerator.createShipDateRandom;
import static worker.tpch.generator.LineItemGenerator.createTaxRandom;
import static worker.tpch.generator.PartGenerator.calculatePartPrice;
import static worker.tpch.util.StringBufferUtil.appendClerk;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;
import static worker.tpch.util.StringBufferUtil.formatDateByDays;

/**
 * port from io.airlift.tpch
 */
public class OrderGenerator extends TableRowGenerator {
    public static final int SCALE_BASE = 1_500_000;

    // portion with have no orders
    public static final int CUSTOMER_MORTALITY = 3;
    static final int LINE_COUNT_MAX = 7;
    private static final int ORDER_DATE_MIN = MIN_GENERATE_DATE;
    private static final int ORDER_DATE_MAX = ORDER_DATE_MIN + (TOTAL_DATE_RANGE - ITEM_SHIP_DAYS - 1);
    public static final int CLERK_SCALE_BASE = 1000;
    private static final int LINE_COUNT_MIN = 1;
    public static final int COMMENT_AVERAGE_LENGTH = 49;

    protected static final int ORDER_KEY_SPARSE_BITS = 2;
    protected static final int ORDER_KEY_SPARSE_KEEP = 3;

    private final RandomBoundedInt orderDateRandom = createOrderDateRandom();
    private final RandomBoundedInt lineCountRandom = createLineCountRandom();
    private final RandomBoundedLong customerKeyRandom;
    private final RandomString orderPriorityRandom;
    private final RandomBoundedInt clerkRandom;
    private final RandomText commentRandom;

    private final RandomBoundedInt lineQuantityRandom = createQuantityRandom();
    private final RandomBoundedInt lineDiscountRandom = createDiscountRandom();
    private final RandomBoundedInt lineTaxRandom = createTaxRandom();
    private final RandomBoundedLong linePartKeyRandom;
    private final RandomBoundedInt lineShipDateRandom = createShipDateRandom();

    private final long maxCustomerKey;

    public OrderGenerator(double scaleFactor, int part, int partCount) {
        this(Distributions.getDefaultDistributions(),
            TextPool.getDefaultTestPool(),
            scaleFactor,
            calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    public OrderGenerator(Distributions distributions, TextPool textPool,
                          double scaleFactor, long startIndex, long rowCount) {
        super(distributions, textPool, startIndex, rowCount);

        clerkRandom =
            new RandomBoundedInt(1171034773, 1, Math.max((int) (scaleFactor * CLERK_SCALE_BASE), CLERK_SCALE_BASE));

        maxCustomerKey = (long) (CustomerGenerator.SCALE_BASE * scaleFactor);
        customerKeyRandom = new RandomBoundedLong(851767375, scaleFactor >= 30000, 1, maxCustomerKey);

        orderPriorityRandom = new RandomString(591449447, distributions.getOrderPriorities());
        commentRandom = new RandomText(276090261, textPool, COMMENT_AVERAGE_LENGTH);

        linePartKeyRandom = createPartKeyRandom(scaleFactor);

        orderDateRandom.advanceRows(startIndex);
        lineCountRandom.advanceRows(startIndex);
        customerKeyRandom.advanceRows(startIndex);
        orderPriorityRandom.advanceRows(startIndex);
        clerkRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);

        lineQuantityRandom.advanceRows(startIndex);
        lineDiscountRandom.advanceRows(startIndex);
        lineShipDateRandom.advanceRows(startIndex);
        lineTaxRandom.advanceRows(startIndex);
        linePartKeyRandom.advanceRows(startIndex);
    }

    public static RandomBoundedInt createLineCountRandom() {
        return new RandomBoundedInt(1434868289, LINE_COUNT_MIN, LINE_COUNT_MAX);
    }

    public static RandomBoundedInt createOrderDateRandom() {
        return new RandomBoundedInt(1066728069, ORDER_DATE_MIN, ORDER_DATE_MAX);
    }

    static long makeOrderKey(long orderIndex) {
        long lowBits = orderIndex & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

        long key = orderIndex;
        key >>= ORDER_KEY_SPARSE_KEEP;
        key <<= ORDER_KEY_SPARSE_BITS;
        key <<= ORDER_KEY_SPARSE_KEEP;
        key += lowBits;

        return key;
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        long orderKey = makeOrderKey(startIndex + index + 1);
        int orderDate = orderDateRandom.nextValue();

        // generate customer key, taking into account customer mortality rate
        long customerKey = customerKeyRandom.nextValue();
        int delta = 1;
        while (customerKey % CUSTOMER_MORTALITY == 0) {
            customerKey += delta;
            customerKey = Math.min(customerKey, maxCustomerKey);
            delta *= -1;
        }

        long totalPrice = 0;
        int shippedCount = 0;

        int lineCount = lineCountRandom.nextValue();
        for (long lineNumber = 0; lineNumber < lineCount; lineNumber++) {
            int quantity = lineQuantityRandom.nextValue();
            int discount = lineDiscountRandom.nextValue();
            int tax = lineTaxRandom.nextValue();

            long partKey = linePartKeyRandom.nextValue();

            long partPrice = calculatePartPrice(partKey);
            long extendedPrice = partPrice * quantity;
            long discountedPrice = extendedPrice * (100 - discount);
            totalPrice += ((discountedPrice / 100) * (100 + tax)) / 100;

            int shipDate = lineShipDateRandom.nextValue();
            shipDate += orderDate;
            if (GenerateUtils.isInPast(shipDate)) {
                shippedCount++;
            }
        }

        char orderStatus;
        if (shippedCount == lineCount) {
            orderStatus = 'F';
        } else if (shippedCount > 0) {
            orderStatus = 'P';
        } else {
            orderStatus = 'O';
        }

        sqlBuffer.append('(').append(orderKey)
            .append(',').append(customerKey)
            .append(",\"").append(orderStatus)
            .append("\",");

        appendDecimalWithFrac2(sqlBuffer, totalPrice);

        sqlBuffer.append(",\"");
        formatDateByDays(sqlBuffer, orderDate - GENERATED_DATE_EPOCH_OFFSET);

        sqlBuffer.append("\",\"").append(orderPriorityRandom.nextValue()).append("\",\"");
        appendClerk(sqlBuffer, clerkRandom.nextValue());
        sqlBuffer.append("\",").append(0)
            .append(",\"").append(commentRandom.nextValue()).append("\"),");

        orderDateRandom.rowFinished();
        lineCountRandom.rowFinished();
        customerKeyRandom.rowFinished();
        orderPriorityRandom.rowFinished();
        clerkRandom.rowFinished();
        commentRandom.rowFinished();

        lineQuantityRandom.rowFinished();
        lineDiscountRandom.rowFinished();
        lineShipDateRandom.rowFinished();
        lineTaxRandom.rowFinished();
        linePartKeyRandom.rowFinished();

        index++;
    }
}
