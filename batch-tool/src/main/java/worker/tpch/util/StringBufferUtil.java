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

public class StringBufferUtil {

    private static final int DAYS_PER_CYCLE = 146097;
    private static final long DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5L) - (30L * 365L + 7L);

    /**
     * 转为两位小数的字符串表示并append
     */
    public static void appendDecimalWithFrac2(StringBuilder sqlBuffer, long l) {
        boolean neg = l < 0;
        if (neg) {
            l = -l;
            sqlBuffer.append('-');
        }
        long intPart = l / 100;
        long fracPart = l % 100;
        if (fracPart < 10) {
            sqlBuffer.append(intPart).append(".0").append(fracPart);
        } else {
            sqlBuffer.append(intPart).append('.').append(fracPart);
        }
    }

    /**
     * Customer#%09d
     */
    public static void appendCustomerName(StringBuilder sqlBuffer, long key) {
        sqlBuffer.append("Customer#");
        append9Digits(sqlBuffer, key);
    }

    /**
     * Customer#%09d
     */
    public static void appendClerk(StringBuilder sqlBuffer, long key) {
        sqlBuffer.append("Clerk#");
        append9Digits(sqlBuffer, key);
    }

    /**
     * Supplier#%09d
     */
    public static void appendSupplier(StringBuilder sqlBuffer, long key) {
        sqlBuffer.append("Supplier#");
        append9Digits(sqlBuffer, key);
    }

    private static void append9Digits(StringBuilder sqlBuffer, long key) {
        if (key >= 100_00) {
            if (key >= 100_000_000) {
                sqlBuffer.append(key);
            } else if (key >= 100_000_00) {
                sqlBuffer.append('0').append(key);
            } else if (key >= 100_000_0) {
                sqlBuffer.append("00").append(key);
            } else if (key >= 100_000) {
                sqlBuffer.append("000").append(key);
            } else {
                sqlBuffer.append("0000").append(key);
            }
        } else {
            if (key >= 100_0) {
                sqlBuffer.append("00000").append(key);
            } else if (key >= 100) {
                sqlBuffer.append("000000").append(key);
            } else if (key >= 10) {
                sqlBuffer.append("0000000").append(key);
            } else if (key >= 0) {
                sqlBuffer.append("00000000").append(key);
            }
        }
    }

    /**
     * 格式 yyyy-MM-dd
     *
     * @param days 自从1970-01-01经过的天数
     */
    public static void formatDateByDays(StringBuilder sqlBuffer, int days) {
        long zeroDay = days + DAYS_0000_TO_1970 - 60;
        long adjust = 0;
        if (zeroDay < 0) {
            long adjustCycles = (zeroDay + 1) / DAYS_PER_CYCLE - 1;
            adjust = adjustCycles * 400;
            zeroDay += -adjustCycles * DAYS_PER_CYCLE;
        }
        long yearEst = (400 * zeroDay + 591) / DAYS_PER_CYCLE;
        long doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        if (doyEst < 0) {
            yearEst--;
            doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        }
        yearEst += adjust;
        int marchDoy0 = (int) doyEst;

        // convert march-based values back to january-based
        int marchMonth0 = (marchDoy0 * 5 + 2) / 153;
        int month = (marchMonth0 + 2) % 12 + 1;
        int dom = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1;
        yearEst += marchMonth0 / 10;

        // skip valid check
        int year = (int) yearEst;

        sqlBuffer.append(year).append('-');
        if (month < 10) {
            sqlBuffer.append('0');
        }
        sqlBuffer.append(month).append('-');
        if (dom < 10) {
            sqlBuffer.append('0');
        }
        sqlBuffer.append(dom);
    }
}
