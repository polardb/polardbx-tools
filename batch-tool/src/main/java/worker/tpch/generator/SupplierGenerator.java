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
import io.airlift.tpch.RandomAlphaNumeric;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomInt;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;
import worker.tpch.util.RandomPhoneNumber;

import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static worker.tpch.util.StringBufferUtil.appendDecimalWithFrac2;
import static worker.tpch.util.StringBufferUtil.appendSupplier;

/**
 * port from io.airlift.tpch
 */
public class SupplierGenerator extends TableRowGenerator {
    public static final int SCALE_BASE = 10_000;
    public static final String BBB_BASE_TEXT = "Customer ";
    public static final String BBB_COMPLAINT_TEXT = "Complaints";
    public static final String BBB_RECOMMEND_TEXT = "Recommends";
    public static final int BBB_COMMENT_LENGTH = BBB_BASE_TEXT.length() + BBB_COMPLAINT_TEXT.length();
    public static final int BBB_COMMENTS_PER_SCALE_BASE = 10;
    public static final int BBB_COMPLAINT_PERCENT = 50;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 63;

    private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(706178559, ADDRESS_AVERAGE_LENGTH);
    private final RandomBoundedInt nationKeyRandom;
    private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(884434366);
    private final RandomBoundedInt accountBalanceRandom =
        new RandomBoundedInt(962338209, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
    private final RandomText commentRandom;
    private final RandomBoundedInt bbbCommentRandom = new RandomBoundedInt(202794285, 1, SCALE_BASE);
    private final RandomInt bbbJunkRandom = new RandomInt(263032577, 1);
    private final RandomInt bbbOffsetRandom = new RandomInt(715851524, 1);
    private final RandomBoundedInt bbbTypeRandom = new RandomBoundedInt(753643799, 0, 100);

    public SupplierGenerator(double scaleFactor, int part, int partCount) {
        this(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool(),
            calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
            calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private SupplierGenerator(Distributions distributions, TextPool textPool, long startIndex, long rowCount) {
        super(distributions, textPool, startIndex, rowCount);

        nationKeyRandom = new RandomBoundedInt(110356601, 0, distributions.getNations().size() - 1);
        commentRandom = new RandomText(1341315363, textPool, COMMENT_AVERAGE_LENGTH);

        addressRandom.advanceRows(startIndex);
        nationKeyRandom.advanceRows(startIndex);
        phoneRandom.advanceRows(startIndex);
        accountBalanceRandom.advanceRows(startIndex);
        commentRandom.advanceRows(startIndex);
        bbbCommentRandom.advanceRows(startIndex);
        bbbJunkRandom.advanceRows(startIndex);
        bbbOffsetRandom.advanceRows(startIndex);
        bbbTypeRandom.advanceRows(startIndex);
    }

    @Override
    public void appendNextRow(StringBuilder sqlBuffer) {
        long supplierKey = startIndex + index + 1;

        String comment = commentRandom.nextValue();
        StringBuilder commentBuffer = null;
        // Add supplier complaints or commendation to the comment
        int bbbCommentRandomValue = bbbCommentRandom.nextValue();
        if (bbbCommentRandomValue <= BBB_COMMENTS_PER_SCALE_BASE) {
            commentBuffer = new StringBuilder(comment);

            // select random place for BBB comment
            int noise = bbbJunkRandom.nextInt(0, (comment.length() - BBB_COMMENT_LENGTH));
            int offset = bbbOffsetRandom.nextInt(0, (comment.length() - (BBB_COMMENT_LENGTH + noise)));

            // select complaint or recommendation
            String type;
            if (bbbTypeRandom.nextValue() < BBB_COMPLAINT_PERCENT) {
                type = BBB_COMPLAINT_TEXT;
            } else {
                type = BBB_RECOMMEND_TEXT;
            }

            // write base text (e.g., "Customer ")
            commentBuffer.replace(offset, offset + BBB_BASE_TEXT.length(), BBB_BASE_TEXT);

            // write complaint or commendation text (e.g., "Complaints" or "Recommends")
            commentBuffer.replace(
                BBB_BASE_TEXT.length() + offset + noise,
                BBB_BASE_TEXT.length() + offset + noise + type.length(),
                type);
        }

        long nationKey = nationKeyRandom.nextValue();

        sqlBuffer.append('(').append(supplierKey).append(",\"");

        appendSupplier(sqlBuffer, supplierKey);
        sqlBuffer.append("\",\"").append(addressRandom.nextValue())
            .append("\",").append(nationKey)
            .append(",\"");

        phoneRandom.appendNextValue(sqlBuffer, nationKey);
        sqlBuffer.append("\",");

        appendDecimalWithFrac2(sqlBuffer, accountBalanceRandom.nextValue());

        if (commentBuffer == null) {
            sqlBuffer.append(",\"").append(comment).append("\"),");
        } else {
            sqlBuffer.append(",\"").append(commentBuffer).append("\"),");
        }

        addressRandom.rowFinished();
        nationKeyRandom.rowFinished();
        phoneRandom.rowFinished();
        accountBalanceRandom.rowFinished();
        commentRandom.rowFinished();
        bbbCommentRandom.rowFinished();
        bbbJunkRandom.rowFinished();
        bbbOffsetRandom.rowFinished();
        bbbTypeRandom.rowFinished();

        index++;
    }
}
