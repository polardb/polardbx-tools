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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.dsl.ProducerType;

import static model.config.GlobalVar.DEFAULT_RING_BUFFER_SIZE;

public class MyWorkerPool {

    public static <T> RingBuffer<T> createRingBuffer(EventFactory<T> factory) {
        return RingBuffer.create(ProducerType.MULTI,
            factory, DEFAULT_RING_BUFFER_SIZE, new BlockingWaitStrategy());
    }

    public static <T> RingBuffer<T> createSingleProducerRingBuffer(EventFactory<T> factory) {
        return RingBuffer.create(ProducerType.SINGLE,
            factory, DEFAULT_RING_BUFFER_SIZE, new BlockingWaitStrategy());
    }

    public static <T> RingBuffer<T> createRingBuffer(EventFactory<T> factory, int bufferSize) {
        return RingBuffer.create(ProducerType.MULTI,
            factory, bufferSize, new BlockingWaitStrategy());
    }

    @SafeVarargs
    public static <T> WorkerPool<T> createWorkerPool(RingBuffer<T> ringBuffer,
                                                     WorkHandler<T>... consumers) {
        SequenceBarrier barriers = ringBuffer.newBarrier();
        WorkerPool<T> workerPool = new WorkerPool<T>(ringBuffer, barriers,
            new EventExceptionHandler(), consumers);
        ringBuffer.addGatingSequences(workerPool.getWorkerSequences());
        return workerPool;
    }
}
