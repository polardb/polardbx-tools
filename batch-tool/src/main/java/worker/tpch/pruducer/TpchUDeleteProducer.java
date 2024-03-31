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

package worker.tpch.pruducer;

import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.Producer;
import worker.tpch.generator.OrderLineDeleteGenerator;
import worker.tpch.model.BatchDeleteSqlEvent;

import java.util.concurrent.ThreadPoolExecutor;

public class TpchUDeleteProducer implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(TpchUDeleteProducer.class);

    private final ProducerExecutionContext context;
    private final RingBuffer<BatchDeleteSqlEvent> ringBuffer;
    private final ThreadPoolExecutor executor;
    private final int scale;
    private final OrderLineDeleteGenerator generator;

    /**
     * @param curRound the n-th round of update, starting from 1
     */
    public TpchUDeleteProducer(ProducerExecutionContext context,
                               RingBuffer<BatchDeleteSqlEvent> ringBuffer,
                               int curRound) {
        this.context = context;
        this.ringBuffer = ringBuffer;
        this.executor = context.getProducerExecutor();;

        this.scale = context.getScale();
        if (scale <= 0) {
            throw new IllegalArgumentException("TPC-H scale must be a positive integer");
        }
        this.generator = new OrderLineDeleteGenerator(scale, curRound);
    }

    @Override
    public void produce() {
        executor.submit(() -> {
            try {
                while (generator.hasData()) {
                    String batchDeleteOrderkey = generator.getBatchDeleteOrderkey();
                    long sequence = ringBuffer.next();
                    BatchDeleteSqlEvent event;
                    try {
                        event = ringBuffer.get(sequence);
                        event.setValues(batchDeleteOrderkey);
                    } finally {
                        context.getEmittedDataCounter().getAndIncrement();
                        ringBuffer.publish(sequence);
                    }
                    generator.next();
                }
                logger.info("TPC-H delete producer has finished");
            } finally {
                context.getCountDownLatch().countDown();
                generator.close();
            }

        });
    }
}
