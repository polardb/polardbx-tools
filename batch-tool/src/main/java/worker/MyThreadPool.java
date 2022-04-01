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

package worker;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyThreadPool {

    private static final int MAX_POOL_SIZE = 2048;
    private static final long ALIVE_TIME = 5 * 1000L;
    private static final int QUEUE_SIZE = 500;

    @Deprecated
    public static ThreadPoolExecutor createExecutor(String name) {
        return new ThreadPoolExecutor(
            MAX_POOL_SIZE,
            MAX_POOL_SIZE,
            ALIVE_TIME,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(QUEUE_SIZE),
            new NamedThreadFactory(name, false));
    }

    public static ThreadPoolExecutor createExecutorWithEnsure(String name, int coreSize) {
        return new ThreadPoolExecutor(
            coreSize + 20,
            coreSize + 20,
            ALIVE_TIME,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(QUEUE_SIZE),
            new NamedThreadFactory(name, false));
    }
}

