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
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomBoundedLong;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;
import worker.tpch.model.TpchTableModel;

import javax.annotation.concurrent.NotThreadSafe;

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
import static worker.tpch.generator.OrderGenerator.LINE_COUNT_MAX;

/**
 * 需要注意与 BatchDeleteGenerator 调用方法的不同
 */
@NotThreadSafe
public abstract class BaseOrderLineBatchInsertGenerator extends BaseOrderLineUpdateGenerator {

    /**
     * insert 20 rows in one sql
     */
    public static final int DEFAULT_INSERT_BATCH_NUM = 20;

    protected final RandomBoundedInt orderDateRandom = OrderGenerator.createOrderDateRandom();
    protected final RandomBoundedInt lineCountRandom = OrderGenerator.createLineCountRandom();

    protected final RandomBoundedLong customerKeyRandom;
    protected final RandomString orderPriorityRandom;
    protected final RandomBoundedInt clerkRandom;
    protected final RandomText ordersCommentRandom;

    protected final RandomBoundedInt lineQuantityRandom = createQuantityRandom();
    protected final RandomBoundedInt lineDiscountRandom = createDiscountRandom();
    protected final RandomBoundedInt lineTaxRandom = createTaxRandom();
    protected final RandomBoundedLong linePartKeyRandom;
    protected final RandomBoundedInt lineShipDateRandom = createShipDateRandom();
    protected final RandomBoundedInt supplierNumberRandom = new RandomBoundedInt(2095021727, 0, 3, LINE_COUNT_MAX);
    protected final RandomBoundedInt lineCommitDateRandom =
        new RandomBoundedInt(904914315, COMMIT_DATE_MIN, COMMIT_DATE_MAX, LINE_COUNT_MAX);
    protected final RandomBoundedInt lineReceiptDateRandom =
        new RandomBoundedInt(373135028, RECEIPT_DATE_MIN, RECEIPT_DATE_MAX, LINE_COUNT_MAX);

    protected final RandomString returnedFlagRandom;
    protected final RandomString shipInstructionsRandom;
    protected final RandomString shipModeRandom;
    protected final RandomText lineCommentRandom;

    protected final long maxCustomerKey;
    protected StringBuilder orderStringBuilder =
        new StringBuilder(DEFAULT_INSERT_BATCH_NUM * TpchTableModel.ORDERS.getRowStrLen() + 32);
    protected StringBuilder lineitemStringBuilder =
        new StringBuilder(DEFAULT_INSERT_BATCH_NUM * TpchTableModel.LINEITEM.getRowStrLen() + 32);

    public BaseOrderLineBatchInsertGenerator(double scaleFactor, int round) {
        super(scaleFactor, round, DEFAULT_INSERT_BATCH_NUM);

        // initialize randoms
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

        advanceRandoms();
    }

    protected abstract void advanceRandoms();

    public String getInsertOrdersSqls() {
        return orderStringBuilder.toString();
    }

    public String getInsertLineitemSqls() {
        return lineitemStringBuilder.toString();
    }

    public abstract void nextBatch();

    @VisibleForTesting
    public abstract void nextRow();

    public boolean hasData() {
        return curIdx < count;
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

}
