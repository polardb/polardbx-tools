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

package model.config;

import model.stat.DebugInfo;

public class GlobalVar {

    /**
     * 发送一批数据的元组数
     */
    public static int EMIT_BATCH_SIZE = 200;

    /**
     * RingBuffer 缓冲区大小
     */
    public static int DEFAULT_RING_BUFFER_SIZE = 1024;

    /**
     * 每个worker线程可分配的堆外内存
     * 4K
     */
    public static int DEFAULT_DIRECT_BUFFER_SIZE_PER_WORKER = 1024 * 4;

    public static boolean IN_PERF_MODE = false;

    public static int DDL_RETRY_COUNT = 3;

    public static int DDL_PARALLELISM = 4;

    public static final boolean DEBUG_MODE = false;

    public static final DebugInfo DEBUG_INFO = new DebugInfo();
}
