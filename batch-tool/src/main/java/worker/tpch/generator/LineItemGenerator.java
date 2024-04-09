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
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomBoundedLong;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import static io.airlift.tpch.GenerateUtils.GENERATED_DATE_EPOCH_OFFSET;
import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static io.airlift.tpch.GenerateUtils.toEpochDate;
import static worker.tpch.generator.OrderGenerator.LINE_COUNT_MAX;
import static worker.tpch.generator.OrderGenerator.createLineCountRandom;
import static worker.tpch.generator.OrderGenerator.createOrderDateRandom;
import static worker.tpch.generator.OrderGenerator.makeOrderKey;
import static worker.tpch.generator.PartSupplierGenerator.selectPartSupplier;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;
import static worker.tpch.util.StringBufferUtil.formatDateByDays;

/**
 * port from io.airlift.tpch
 */
public class LineItemGenerator extends TableRowGenerator {
    private static final int QUANTITY_MIN = 1;
    private static final int QUANTITY_MAX = 50;
    private static final int TAX_MIN = 0;
    private static final int TAX_MAX = 8;
    private static final int DISCOUNT_MIN = 0;
    private static final int DISCOUNT_MAX = 10;
    private static final int PART_KEY_MIN = 1;

    public static final int SHIP_DATE_MIN = 1;
    public static final int SHIP_DATE_MAX = 121;
    public static final int COMMIT_DATE_MIN = 30;
    public static final int COMMIT_DATE_MAX = 90;
    public static final int RECEIPT_DATE_MIN = 1;
    public static final int RECEIPT_DATE_MAX = 30;

    static final int ITEM_SHIP_DAYS = SHIP_DATE_MAX + RECEIPT_DATE_MAX;

    public static final int COMMENT_AVERAGE_LENGTH = 27;

    private final RandomBoundedInt orderDateRandom = createOrderDateRandom();
    private final RandomBoundedInt lineCountRandom = createLineCountRandom();

    private final RandomBoundedInt quantityRandom = createQuantityRandom();
    private final RandomBoundedInt discountRandom = createDiscountRandom();
    private final RandomBoundedInt taxRandom = createTaxRandom();

    private final RandomBoundedLong linePartKeyRandom;

    private final RandomBoundedInt supplierNumberRandom = new RandomBoundedInt(2095021727, 0, 3, LINE_COUNT_MAX);

    private final RandomBoundedInt shipDateRandom = createShipDateRandom();
    private final RandomBoundedInt commitDateRandom =
        new RandomBoundedInt(904914315, COMMIT_DATE_MIN, COMMIT_DATE_MAX, LINE_COUNT_MAX);
    private final RandomBoundedInt receiptDateRandom =
        new RandomBoundedInt(373135028, RECEIPT_DATE_MIN, RECEIPT_DATE_MAX, LINE_COUNT_MAX);

    private final RandomString returnedFlagRandom;
    private final RandomString shipInstructionsRandom;
    private final RandomString shipModeRandom;

    private final RandomText commentRandom;

    private final double scaleFactor;

    private int orderDate;
    private int lineCount;
    private int lineNumber;

    public LineItemGenerator(double scaleFactor, int part, int partCount) {
        this(Distributions.getDefaultDistributions(),
            TextPool.getDefaultTestPool(),
            scaleFactor,
            calculateStartIndex(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount));
    }

    public LineItemGenerator(Distributions distributions, TextPool textPool,
                             double scaleFactor,
                             long startIndex, long rowCount) {
        super(distributions, textPool, startIndex, rowCount);
        this.scaleFactor = scaleFactor;

        returnedFlagRandom = new RandomString(717419739, distributions.getReturnFlags(), LINE_COUNT_MAX);
        shipInstructionsRandom = new RandomString(1371272478, distributions.getShipInstructions(), LINE_COUNT_MAX);
        shipModeRandom = new RandomString(675466456, distributions.getShipModes(), LINE_COUNT_MAX);
        commentRandom = new RandomText(1095462486, textPool, COMMENT_AVERAGE_LENGTH, LINE_COUNT_MAX);

        linePartKeyRandom = createPartKeyRandom(scaleFactor);

        orderDateRandom.advanceRows(startIndex);
        lineCountRandom.advanceRows(startIndex);

        quantityRandom.advanceRows(startIndex);
        discountRandom.advanceRows(startIndex);
        taxRandom.advanceRows(startIndex);

        linePartKeyRandom.advanceRows(startIndex);

        supplierNumberRandom.advanceRows(startIndex);

        shipDateRandom.advanceRows(startIndex);
        commitDateRandom.advanceRows(startIndex);
        receiptDateRandom.advanceRows(startIndex);

        returnedFlagRandom.advanceRows(startIndex);
        shipInstructionsRandom.advanceRows(startIndex);
        shipModeRandom.advanceRows(startIndex);

        commentRandom.advanceRows(startIndex);

        // generate information for initial order
        orderDate = orderDateRandom.nextValue();
        lineCount = lineCountRandom.nextValue() - 1;
    }

    static RandomBoundedInt createQuantityRandom() {
        return new RandomBoundedInt(209208115, QUANTITY_MIN, QUANTITY_MAX, LINE_COUNT_MAX);
    }

    static RandomBoundedInt createDiscountRandom() {
        return new RandomBoundedInt(554590007, DISCOUNT_MIN, DISCOUNT_MAX, LINE_COUNT_MAX);
    }

    static RandomBoundedInt createTaxRandom() {
        return new RandomBoundedInt(721958466, TAX_MIN, TAX_MAX, LINE_COUNT_MAX);
    }

    static RandomBoundedLong createPartKeyRandom(double scaleFactor) {
        return new RandomBoundedLong(1808217256, scaleFactor >= 30000, PART_KEY_MIN,
            (long) (PartGenerator.SCALE_BASE * scaleFactor), LINE_COUNT_MAX);
    }

    static RandomBoundedInt createShipDateRandom() {
        return new RandomBoundedInt(1769349045, SHIP_DATE_MIN, SHIP_DATE_MAX, LINE_COUNT_MAX);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        long orderKey = makeOrderKey(startIndex + index + 1);
        int quantity = quantityRandom.nextValue();
        int discount = discountRandom.nextValue();
        int tax = taxRandom.nextValue();

        long partKey = linePartKeyRandom.nextValue();

        int supplierNumber = supplierNumberRandom.nextValue();
        long supplierKey = selectPartSupplier(partKey, supplierNumber, scaleFactor);

        long partPrice = PartGenerator.calculatePartPrice(partKey);
        long extendedPrice = partPrice * quantity;

        int shipDate = shipDateRandom.nextValue();
        shipDate += orderDate;
        int commitDate = commitDateRandom.nextValue();
        commitDate += orderDate;
        int receiptDate = receiptDateRandom.nextValue();
        receiptDate += shipDate;

        shipDate = toEpochDate(shipDate);
        commitDate = toEpochDate(commitDate);
        receiptDate = toEpochDate(receiptDate);

        String returnedFlag;
        if (GenerateUtils.isInPast(receiptDate + GENERATED_DATE_EPOCH_OFFSET)) {
            returnedFlag = returnedFlagRandom.nextValue();
        } else {
            returnedFlag = "N";
        }

        String status;
        if (GenerateUtils.isInPast(shipDate + GENERATED_DATE_EPOCH_OFFSET)) {
            status = "F";
        } else {
            status = "O";
        }

        String shipInstructions = shipInstructionsRandom.nextValue();
        String shipMode = shipModeRandom.nextValue();
        String comment = commentRandom.nextValue();

        sqlBuffer.append('(').append(orderKey)
            .append(',').append(partKey)
            .append(',').append(supplierKey)
            .append(',').append(lineNumber + 1)
            .append(',').append(quantity)
            .append(",");

        appendDecimalWithFrac2(sqlBuffer, extendedPrice);
        sqlBuffer.append(',');
        appendDecimalWithFrac2(sqlBuffer, discount);
        sqlBuffer.append(',');
        appendDecimalWithFrac2(sqlBuffer, tax);

        sqlBuffer.append(",\"").append(returnedFlag)
            .append("\",\"").append(status)
            .append("\",\"");

        formatDateByDays(sqlBuffer, shipDate);
        sqlBuffer.append("\",\"");
        formatDateByDays(sqlBuffer, commitDate);
        sqlBuffer.append("\",\"");
        formatDateByDays(sqlBuffer, receiptDate);
        sqlBuffer.append("\",\"").append(shipInstructions)
            .append("\",\"").append(shipMode)
            .append("\",\"").append(comment).append("\"),");

        lineNumber++;

        // advance next row only when all lines for the order have been produced
        if (lineNumber > lineCount) {
            orderDateRandom.rowFinished();

            lineCountRandom.rowFinished();
            quantityRandom.rowFinished();
            discountRandom.rowFinished();
            taxRandom.rowFinished();

            linePartKeyRandom.rowFinished();

            supplierNumberRandom.rowFinished();

            shipDateRandom.rowFinished();
            commitDateRandom.rowFinished();
            receiptDateRandom.rowFinished();

            returnedFlagRandom.rowFinished();
            shipInstructionsRandom.rowFinished();
            shipModeRandom.rowFinished();

            commentRandom.rowFinished();

            index++;

            // generate information for next order
            lineCount = lineCountRandom.nextValue() - 1;
            orderDate = orderDateRandom.nextValue();
            lineNumber = 0;
        }
    }
}
