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

import io.airlift.tpch.GenerateUtils;
import worker.tpch.model.TpchTableModel;

import javax.annotation.concurrent.NotThreadSafe;

import static io.airlift.tpch.GenerateUtils.GENERATED_DATE_EPOCH_OFFSET;
import static io.airlift.tpch.GenerateUtils.toEpochDate;
import static worker.tpch.generator.OrderGenerator.CUSTOMER_MORTALITY;
import static worker.tpch.generator.PartSupplierGenerator.selectPartSupplier;
import static worker.tpch.util.StringBufferUtil.appendClerk;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;
import static worker.tpch.util.StringBufferUtil.formatDateByDays;

@NotThreadSafe
public class OrderLineInsertForRollbackGenerator extends BaseOrderLineBatchInsertGenerator {

    public OrderLineInsertForRollbackGenerator(double scaleFactor, int round) {
        super(scaleFactor, round);
    }

    @Override
    protected void advanceRandoms() {
        orderDateRandom.advanceRows(startIndex);
        lineCountRandom.advanceRows(startIndex);
        customerKeyRandom.advanceRows(startIndex);
        orderPriorityRandom.advanceRows(startIndex);
        clerkRandom.advanceRows(startIndex);
        ordersCommentRandom.advanceRows(startIndex);

        supplierNumberRandom.advanceRows(startIndex);

        lineQuantityRandom.advanceRows(startIndex);
        lineDiscountRandom.advanceRows(startIndex);
        lineShipDateRandom.advanceRows(startIndex);
        lineTaxRandom.advanceRows(startIndex);
        linePartKeyRandom.advanceRows(startIndex);
        lineCommitDateRandom.advanceRows(startIndex);
        lineReceiptDateRandom.advanceRows(startIndex);
        returnedFlagRandom.advanceRows(startIndex);
        shipInstructionsRandom.advanceRows(startIndex);
        shipModeRandom.advanceRows(startIndex);
        lineCommentRandom.advanceRows(startIndex);
    }

    @Override
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
            long orderKey = makeDeleteOrderKey(i);
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

    @Override
    public void nextRow() {
        orderStringBuilder.setLength(0);
        lineitemStringBuilder.setLength(0);

        long orderKey = makeDeleteOrderKey(startIndex + curIdx + 1);
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
}
