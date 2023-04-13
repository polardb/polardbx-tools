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

package worker.tpch.util;

import io.airlift.tpch.AbstractRandomInt;

public class RandomAlphaNumeric
    extends AbstractRandomInt {
    private static final char[] ALPHA_NUMERIC =
        "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,".toCharArray();

    private static final double LOW_LENGTH_MULTIPLIER = 0.4;
    private static final double HIGH_LENGTH_MULTIPLIER = 1.6;

    private static final int USAGE_PER_ROW = 9;

    private final int minLength;
    private final int maxLength;

    public RandomAlphaNumeric(long seed, int averageLength) {
        this(seed, averageLength, 1);
    }

    public RandomAlphaNumeric(long seed, int averageLength, int expectedRowCount) {
        super(seed, USAGE_PER_ROW * expectedRowCount);
        this.minLength = (int) (averageLength * LOW_LENGTH_MULTIPLIER);
        this.maxLength = (int) (averageLength * HIGH_LENGTH_MULTIPLIER);
    }

    public void appendNextValue(StringBuilder stringBuilder) {
        int len = nextInt(minLength, maxLength);

        long charIndex = 0;
        for (int i = 0; i < len; i++) {
            if (i % 5 == 0) {
                charIndex = nextInt(0, Integer.MAX_VALUE);
            }
            stringBuilder.append(ALPHA_NUMERIC[(int) (charIndex & 0x3f)]);
            charIndex >>= 6;
        }
    }
}
