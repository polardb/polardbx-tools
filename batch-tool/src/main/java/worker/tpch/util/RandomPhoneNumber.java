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

public class RandomPhoneNumber
    extends AbstractRandomInt {
    // limited by country codes in phone numbers
    private static final int NATIONS_MAX = 90;

    public RandomPhoneNumber(long seed) {
        this(seed, 1);
    }

    public RandomPhoneNumber(long seed, int expectedRowCount) {
        super(seed, 3 * expectedRowCount);
    }

    public void appendNextValue(StringBuilder stringBuilder, long nationKey) {
        stringBuilder.append((10 + (nationKey % NATIONS_MAX)))
            .append('-').append(nextInt(100, 999))
            .append('-').append(nextInt(100, 999))
            .append('-').append(nextInt(1000, 9999));
    }
}
