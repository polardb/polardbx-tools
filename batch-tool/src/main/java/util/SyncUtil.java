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

package util;

import model.config.GlobalVar;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncUtil {

    public static CountDownLatch newMainCountDownLatch(int count) {
        CountDownLatch countDownLatch = new CountDownLatch(count);
        GlobalVar.DEBUG_INFO.setCountDownLatch(countDownLatch);
        return countDownLatch;
    }

    public static AtomicInteger newRemainDataCounter() {
        AtomicInteger remainDataCounter = new AtomicInteger(0);
        GlobalVar.DEBUG_INFO.setRemainDataCounter(remainDataCounter);
        return remainDataCounter;
    }

    public static void waitForFinish(CountDownLatch countDownLatch, AtomicInteger remainDataCounter)
        throws InterruptedException {
        // 等待生产者结束
        countDownLatch.await();
        // 等待消费者消费完成
        int remain;
        while ((remain = remainDataCounter.get()) > 0) {
            Thread.sleep(500);
        }
    }
}
