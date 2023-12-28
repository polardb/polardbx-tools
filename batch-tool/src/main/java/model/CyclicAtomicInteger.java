/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package model;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class CyclicAtomicInteger {
    private final static long PARK_TIME = 1000 * 1000L;
    private final AtomicInteger counter;
    private final int range;

    public CyclicAtomicInteger(int range) {
        this.counter = new AtomicInteger(0);
        this.range = range;
    }

    public CyclicAtomicInteger(int initialValue, int range) {
        this.counter = new AtomicInteger(initialValue);
        this.range = range;
    }

    /**
     * 获取下一个值
     */
    public int next() {
        int c, next;
        for (; ; ) {
            c = counter.get();
            next = (c + 1) % range;
            if (counter.compareAndSet(c, next)) {
                return c;
            } else {
                LockSupport.parkNanos(PARK_TIME);
            }
        }
    }
}
