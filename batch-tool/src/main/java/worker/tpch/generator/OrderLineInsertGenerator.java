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
import io.airlift.tpch.CustomerGenerator;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomBoundedLong;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;
import worker.tpch.model.TpchTableModel;

import javax.annotation.concurrent.NotThreadSafe;

import static io.airlift.tpch.GenerateUtils.GENERATED_DATE_EPOCH_OFFSET;
import static io.airlift.tpch.GenerateUtils.toEpochDate;
import static worker.tpch.generator.LineItemGenerator.COMMIT_DATE_MAX;
import static worker.tpch.generator.LineItemGenerator.COMMIT_DATE_MIN;
import static worker.tpch.generator.LineItemGenerator.RECEIPT_DATE_MAX;
import static worker.tpch.generator.LineItemGenerator.RECEIPT_DATE_MIN;
import static worker.tpch.generator.LineItemGenerator.createDiscountRandom;
import static worker.tpch.generator.LineItemGenerator.createPartKeyRandom;
import static worker.tpch.generator.LineItemGenerator.createQuantityRandom;
import static worker.tpch.generator.LineItemGenerator.createShipDateRandom;
import static worker.tpch.generator.LineItemGenerator.createTaxRandom;
import static worker.tpch.generator.OrderGenerator.CLERK_SCALE_BASE;
import static worker.tpch.generator.OrderGenerator.CUSTOMER_MORTALITY;
import static worker.tpch.generator.OrderGenerator.LINE_COUNT_MAX;
import static worker.tpch.generator.OrderGenerator.ORDER_KEY_SPARSE_BITS;
import static worker.tpch.generator.OrderGenerator.ORDER_KEY_SPARSE_KEEP;
import static worker.tpch.generator.PartSupplierGenerator.selectPartSupplier;
import static worker.tpch.util.StringBufferUtil.appendClerk;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;
import static worker.tpch.util.StringBufferUtil.formatDateByDays;

/**
 * 需要注意与 OrderLineDeleteGenerator 调用方法的不同
 */
@NotThreadSafe
public class OrderLineInsertGenerator extends BaseOrderLineUpdateGenerator {

    private static final int STEPS = 101;
    private static final int PROCS = 100;

    private final RandomBoundedInt orderDateRandom = OrderGenerator.createOrderDateRandom();
    private final RandomBoundedInt lineCountRandom = OrderGenerator.createLineCountRandom();

    private final RandomBoundedLong customerKeyRandom;
    private final RandomString orderPriorityRandom;
    private final RandomBoundedInt clerkRandom;
    private final RandomText ordersCommentRandom;

    private final RandomBoundedInt lineQuantityRandom = createQuantityRandom();
    private final RandomBoundedInt lineDiscountRandom = createDiscountRandom();
    private final RandomBoundedInt lineTaxRandom = createTaxRandom();
    private final RandomBoundedLong linePartKeyRandom;
    private final RandomBoundedInt lineShipDateRandom = createShipDateRandom();
    private final RandomBoundedInt supplierNumberRandom = new RandomBoundedInt(2095021727, 0, 3, LINE_COUNT_MAX);
    private final RandomBoundedInt lineCommitDateRandom =
        new RandomBoundedInt(904914315, COMMIT_DATE_MIN, COMMIT_DATE_MAX, LINE_COUNT_MAX);
    private final RandomBoundedInt lineReceiptDateRandom =
        new RandomBoundedInt(373135028, RECEIPT_DATE_MIN, RECEIPT_DATE_MAX, LINE_COUNT_MAX);

    private final RandomString returnedFlagRandom;
    private final RandomString shipInstructionsRandom;
    private final RandomString shipModeRandom;
    private final RandomText lineCommentRandom;

    private final long maxCustomerKey;
    private StringBuilder orderStringBuilder =
        new StringBuilder(DEFAULT_INSERT_BATCH_NUM * TpchTableModel.ORDERS.getRowStrLen() + 32);
    private StringBuilder lineitemStringBuilder =
        new StringBuilder(DEFAULT_INSERT_BATCH_NUM * TpchTableModel.LINEITEM.getRowStrLen() + 32);

    public OrderLineInsertGenerator(double scaleFactor, int round) {
        super(scaleFactor, round, DEFAULT_DELETE_BATCH_NUM);

        maxCustomerKey = (long) (CustomerGenerator.SCALE_BASE * scaleFactor);
        customerKeyRandom = new RandomBoundedLong(851767375,
            scaleFactor >= 30000, 1, maxCustomerKey);
        Distributions distributions = Distributions.getDefaultDistributions();
        orderPriorityRandom = new RandomString(591449447, distributions.getOrderPriorities());
        TextPool orderTextPool = TextPool.getDefaultTestPool();
        ordersCommentRandom = new RandomText(276090261, orderTextPool, OrderGenerator.COMMENT_AVERAGE_LENGTH);
        clerkRandom =
            new RandomBoundedInt(1171034773, 1, Math.max((int) (scaleFactor * CLERK_SCALE_BASE), CLERK_SCALE_BASE));
        linePartKeyRandom = createPartKeyRandom(scaleFactor);

        returnedFlagRandom = new RandomString(717419739, distributions.getReturnFlags(), LINE_COUNT_MAX);
        shipInstructionsRandom = new RandomString(1371272478, distributions.getShipInstructions(), LINE_COUNT_MAX);
        shipModeRandom = new RandomString(675466456, distributions.getShipModes(), LINE_COUNT_MAX);
        TextPool lineTextPool = TextPool.getDefaultTestPool();
        lineCommentRandom =
            new RandomText(1095462486, lineTextPool, LineItemGenerator.COMMENT_AVERAGE_LENGTH, LINE_COUNT_MAX);

        long rowcount = (long) (OrderGenerator.SCALE_BASE * scaleFactor);
        long extra = rowcount % PROCS;
        rowcount /= PROCS;

        for (int i = 0; i < STEPS - 1; i++) {
            orderDateRandom.advanceRows(rowcount);
            lineCountRandom.advanceRows(rowcount);
            customerKeyRandom.advanceRows(rowcount);
            orderPriorityRandom.advanceRows(rowcount);
            clerkRandom.advanceRows(rowcount);
            ordersCommentRandom.advanceRows(rowcount);

            supplierNumberRandom.advanceRows(rowcount);

            lineQuantityRandom.advanceRows(rowcount);
            lineDiscountRandom.advanceRows(rowcount);
            lineShipDateRandom.advanceRows(rowcount);
            lineTaxRandom.advanceRows(rowcount);
            linePartKeyRandom.advanceRows(rowcount);
            lineCommitDateRandom.advanceRows(rowcount);
            lineReceiptDateRandom.advanceRows(rowcount);
            returnedFlagRandom.advanceRows(rowcount);
            shipInstructionsRandom.advanceRows(rowcount);
            shipModeRandom.advanceRows(rowcount);
            lineCommentRandom.advanceRows(rowcount);
        }

        if (extra != 0) {
            orderDateRandom.advanceRows(extra);
            lineCountRandom.advanceRows(extra);
            customerKeyRandom.advanceRows(extra);
            orderPriorityRandom.advanceRows(extra);
            clerkRandom.advanceRows(extra);
            ordersCommentRandom.advanceRows(extra * 2);
        }

        if (startIndex != 0) {
            orderDateRandom.advanceRows(startIndex);
            lineCountRandom.advanceRows(startIndex);
            customerKeyRandom.advanceRows(startIndex);
            orderPriorityRandom.advanceRows(startIndex);
            clerkRandom.advanceRows(startIndex);
            ordersCommentRandom.advanceRows(startIndex);

            supplierNumberRandom.advanceRows(startIndex);

            lineQuantityRandom.advanceRows(startIndex);
            lineDiscountRandom.advanceRows(startIndex);
            lineTaxRandom.advanceRows(startIndex);
            linePartKeyRandom.advanceRows(startIndex);
            lineShipDateRandom.advanceRows(startIndex);
            lineCommitDateRandom.advanceRows(startIndex);
            lineReceiptDateRandom.advanceRows(startIndex);
            returnedFlagRandom.advanceRows(startIndex);
            shipInstructionsRandom.advanceRows(startIndex);
            shipModeRandom.advanceRows(startIndex);
            lineCommentRandom.advanceRows(startIndex);
        }
    }

    static long makeInsertOrderKey(long orderIndex) {
        long lowBits = orderIndex & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

        long key = orderIndex;
        key >>= ORDER_KEY_SPARSE_KEEP;
        key <<= ORDER_KEY_SPARSE_BITS;
        key += 1;
        key <<= ORDER_KEY_SPARSE_KEEP;
        key += lowBits;

        return key;
    }

    public String getInsertOrdersSqls() {
        return orderStringBuilder.toString();
    }

    public String getInsertLineitemSqls() {
        return lineitemStringBuilder.toString();
    }

    public void nextBatch() {
        long start = startIndex + curIdx + 1;
        long remain = Math.min(endIndex - start + 1, batchSize);
        if (remain <= 0) {
            throw new IllegalStateException("No more data in this batch");
        }
        orderStringBuilder.setLength(0);
        lineitemStringBuilder.setLength(0);
        orderStringBuilder.append("INSERT INTO ").append(TpchTableModel.ORDERS.getName()).append(" VALUES ");
        lineitemStringBuilder.append("INSERT INTO ").append(TpchTableModel.LINEITEM.getName()).append(" VALUES ");

        for (long i = start; remain > 0; i++, remain--) {
            long orderKey = makeInsertOrderKey(i);
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
            for (int lineNumber = 0; lineNumber < lineCount; lineNumber++) {
                int quantity = lineQuantityRandom.nextValue();
                int discount = lineDiscountRandom.nextValue();
                int tax = lineTaxRandom.nextValue();

                long partKey = linePartKeyRandom.nextValue();

                long partPrice = PartGenerator.calculatePartPrice(partKey);
                long extendedPrice = partPrice * quantity;
                long discountedPrice = extendedPrice * (100 - discount);
                totalPrice += ((discountedPrice / 100) * (100 + tax)) / 100;

                int shipDate = lineShipDateRandom.nextValue();
                shipDate += orderDate;
                if (GenerateUtils.isInPast(shipDate)) {
                    shippedCount++;
                }
                int commitDate = lineCommitDateRandom.nextValue();
                commitDate += orderDate;
                int receiptDate = lineReceiptDateRandom.nextValue();
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
                String comment = lineCommentRandom.nextValue();

                int supplierNumber = supplierNumberRandom.nextValue();
                long supplierKey = selectPartSupplier(partKey, supplierNumber, scaleFactor);
                lineitemStringBuilder.append('(').append(orderKey)
                    .append(',').append(partKey)
                    .append(',').append(supplierKey)
                    .append(',').append(lineNumber + 1)
                    .append(',').append(quantity)
                    .append(",");

                appendDecimalWithFrac2(lineitemStringBuilder, extendedPrice);
                lineitemStringBuilder.append(',');
                appendDecimalWithFrac2(lineitemStringBuilder, discount);
                lineitemStringBuilder.append(',');
                appendDecimalWithFrac2(lineitemStringBuilder, tax);
                lineitemStringBuilder.append(",\"").append(returnedFlag)
                    .append("\",\"").append(status)
                    .append("\",\"");

                formatDateByDays(lineitemStringBuilder, shipDate);
                lineitemStringBuilder.append("\",\"");
                formatDateByDays(lineitemStringBuilder, commitDate);
                lineitemStringBuilder.append("\",\"");
                formatDateByDays(lineitemStringBuilder, receiptDate);
                lineitemStringBuilder.append("\",\"").append(shipInstructions)
                    .append("\",\"").append(shipMode)
                    .append("\",\"").append(comment).append("\"),");
            }

            char orderStatus;
            if (shippedCount == lineCount) {
                orderStatus = 'F';
            } else if (shippedCount > 0) {
                orderStatus = 'P';
            } else {
                orderStatus = 'O';
            }

            orderStringBuilder.append('(').append(orderKey)
                .append(',').append(customerKey)
                .append(",\"").append(orderStatus)
                .append("\",");

            appendDecimalWithFrac2(orderStringBuilder, totalPrice);

            orderStringBuilder.append(",\"");
            formatDateByDays(orderStringBuilder, orderDate - GENERATED_DATE_EPOCH_OFFSET);

            orderStringBuilder.append("\",\"").append(orderPriorityRandom.nextValue()).append("\",\"");
            appendClerk(orderStringBuilder, clerkRandom.nextValue());
            orderStringBuilder.append("\",").append(0)
                .append(",\"").append(ordersCommentRandom.nextValue()).append("\"),");

            orderDateRandom.rowFinished();
            lineCountRandom.rowFinished();
            customerKeyRandom.rowFinished();
            orderPriorityRandom.rowFinished();
            clerkRandom.rowFinished();
            ordersCommentRandom.rowFinished();

            lineQuantityRandom.rowFinished();
            lineDiscountRandom.rowFinished();
            lineShipDateRandom.rowFinished();
            lineTaxRandom.rowFinished();
            linePartKeyRandom.rowFinished();

            supplierNumberRandom.rowFinished();
            lineCommitDateRandom.rowFinished();
            lineReceiptDateRandom.rowFinished();

            returnedFlagRandom.rowFinished();
            shipInstructionsRandom.rowFinished();
            shipModeRandom.rowFinished();

            lineCommentRandom.rowFinished();
            curIdx++;
        }

        orderStringBuilder.setCharAt(orderStringBuilder.length() - 1, ';');
        lineitemStringBuilder.setCharAt(lineitemStringBuilder.length() - 1, ';');
    }

    public void close() {
        this.lineitemStringBuilder = null;
        this.orderStringBuilder = null;
    }

    @VisibleForTesting
    public String getLineitemRows() {
        return lineitemStringBuilder.toString();
    }

    @VisibleForTesting
    public String getOrderRow() {
        return orderStringBuilder.toString();
    }

    @VisibleForTesting
    public void nextRow() {
        orderStringBuilder.setLength(0);
        lineitemStringBuilder.setLength(0);

        long orderKey = makeInsertOrderKey(startIndex + curIdx + 1);
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

            long partPrice = PartGenerator.calculatePartPrice(partKey);
            long extendedPrice = partPrice * quantity;
            long discountedPrice = extendedPrice * (100 - discount);
            totalPrice += ((discountedPrice / 100) * (100 + tax)) / 100;

            int shipDate = lineShipDateRandom.nextValue();
            shipDate += orderDate;
            if (GenerateUtils.isInPast(shipDate)) {
                shippedCount++;
            }
            int commitDate = lineCommitDateRandom.nextValue();
            commitDate += orderDate;
            int receiptDate = lineReceiptDateRandom.nextValue();
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
            String comment = lineCommentRandom.nextValue();

            int supplierNumber = supplierNumberRandom.nextValue();
            long supplierKey = selectPartSupplier(partKey, supplierNumber, scaleFactor);
            lineitemStringBuilder.append(orderKey)
                .append('|').append(partKey)
                .append('|').append(supplierKey)
                .append('|').append(lineNumber + 1)
                .append('|').append(quantity)
                .append("|");
            appendDecimalWithFrac2(lineitemStringBuilder, extendedPrice);
            lineitemStringBuilder.append('|');
            appendDecimalWithFrac2(lineitemStringBuilder, discount);
            lineitemStringBuilder.append('|');
            appendDecimalWithFrac2(lineitemStringBuilder, tax);
            lineitemStringBuilder.append('|').append(returnedFlag)
                .append('|').append(status)
                .append('|');

            formatDateByDays(lineitemStringBuilder, shipDate);
            lineitemStringBuilder.append('|');
            formatDateByDays(lineitemStringBuilder, commitDate);
            lineitemStringBuilder.append('|');
            formatDateByDays(lineitemStringBuilder, receiptDate);
            lineitemStringBuilder.append('|').append(shipInstructions)
                .append('|').append(shipMode)
                .append('|').append(comment).append('|');
            if (lineNumber < lineCount - 1) {
                lineitemStringBuilder.append('\n');
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

        orderStringBuilder.append(orderKey)
            .append('|').append(customerKey)
            .append('|').append(orderStatus)
            .append('|');

        appendDecimalWithFrac2(orderStringBuilder, totalPrice);

        orderStringBuilder.append("|");
        formatDateByDays(orderStringBuilder, orderDate - GENERATED_DATE_EPOCH_OFFSET);

        orderStringBuilder.append("|").append(orderPriorityRandom.nextValue()).append("|");
        appendClerk(orderStringBuilder, clerkRandom.nextValue());
        orderStringBuilder.append("|").append(0)
            .append("|").append(ordersCommentRandom.nextValue()).append("|");

        orderDateRandom.rowFinished();
        lineCountRandom.rowFinished();
        customerKeyRandom.rowFinished();
        orderPriorityRandom.rowFinished();
        clerkRandom.rowFinished();
        ordersCommentRandom.rowFinished();

        lineQuantityRandom.rowFinished();
        lineDiscountRandom.rowFinished();
        lineShipDateRandom.rowFinished();
        lineTaxRandom.rowFinished();
        linePartKeyRandom.rowFinished();

        supplierNumberRandom.rowFinished();
        lineCommitDateRandom.rowFinished();
        lineReceiptDateRandom.rowFinished();

        returnedFlagRandom.rowFinished();
        shipInstructionsRandom.rowFinished();
        shipModeRandom.rowFinished();

        lineCommentRandom.rowFinished();
        this.curIdx++;
    }

    public boolean hasData() {
        return curIdx < count;
    }
}
