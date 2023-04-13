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
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;
import worker.tpch.util.RandomAlphaNumeric;
import worker.tpch.util.RandomPhoneNumber;

import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static worker.tpch.util.StringBufferUtil.appendCustomerName;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;

/**
 * port from io.airlift.tpch
 */
public class CustomerGenerator extends TableRowGenerator {
    public static final int SCALE_BASE = 150_000;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 73;

    private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
    private final RandomBoundedInt nationKeyRandom;
    private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
    private final RandomBoundedInt accountBalanceRandom =
        new RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
    private final RandomString marketSegmentRandom;
    private final RandomText commentRandom;

    public CustomerGenerator(double scaleFactor, int part, int partCount) {
        this(Distributions.getDefaultDistributions(),
            TextPool.getDefaultTestPool(),
            calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    public CustomerGenerator(Distributions distributions, TextPool textPool, long startIndex, long rowCount) {
        super(distributions, textPool, startIndex, rowCount);

        nationKeyRandom = new RandomBoundedInt(1489529863, 0, distributions.getNations().size() - 1);
        marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
        commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

        addressRandom.advanceRows(startIndex);
        nationKeyRandom.advanceRows(startIndex);
        phoneRandom.advanceRows(startIndex);
        accountBalanceRandom.advanceRows(startIndex);
        marketSegmentRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {

        long nationKey = nationKeyRandom.nextValue();
        long customerKey = startIndex + index + 1;

        sqlBuffer.append('(').append(customerKey)   // c_custkey
            .append(",\"");

        appendCustomerName(sqlBuffer, customerKey); // c_name

        sqlBuffer.append("\",\"");
        addressRandom.appendNextValue(sqlBuffer);   // c_address
        sqlBuffer.append("\",").append(nationKey).append(",\"");    // c_nationkey
        phoneRandom.appendNextValue(sqlBuffer, nationKey);  // c_phone
        sqlBuffer.append("\",");

        appendDecimalWithFrac2(sqlBuffer, accountBalanceRandom.nextValue());    // c_acctbal

        sqlBuffer.append(",\"").append(marketSegmentRandom.nextValue()) // c_mktsegment
            .append("\",\"").append(commentRandom.nextValue()).append("\"),");  // c_comment

        addressRandom.rowFinished();
        nationKeyRandom.rowFinished();
        phoneRandom.rowFinished();
        accountBalanceRandom.rowFinished();
        marketSegmentRandom.rowFinished();
        commentRandom.rowFinished();

        index++;
    }
}
