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
import worker.tpch.generator.OrderLineInsertGenerator;
import worker.tpch.model.BatchInsertSql2Event;

import java.util.concurrent.ThreadPoolExecutor;

public class TpchUInsertProducer implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(TpchUInsertProducer.class);

    private final ProducerExecutionContext context;
    private final RingBuffer<BatchInsertSql2Event> ringBuffer;
    private final ThreadPoolExecutor executor;
    private final int scale;
    private final OrderLineInsertGenerator generator;

    /**
     * @param curRound the n-th round of update, starting from 1
     */
    public TpchUInsertProducer(ProducerExecutionContext context,
                               RingBuffer<BatchInsertSql2Event> ringBuffer,
                               int curRound) {
        this.context = context;
        this.ringBuffer = ringBuffer;
        this.executor = context.getProducerExecutor();

        this.scale = context.getScale();
        if (scale <= 0) {
            throw new IllegalArgumentException("TPC-H scale must be a positive integer");
        }
        this.generator = new OrderLineInsertGenerator(scale, curRound);
    }

    @Override
    public void produce() {
        executor.submit(() -> {
            try {
                while (generator.hasData()) {
                    generator.nextBatch();

                    String insertOrdersSqls = generator.getInsertOrdersSqls();
                    String insertLineitemSqls = generator.getInsertLineitemSqls();
                    long sequence = ringBuffer.next();
                    BatchInsertSql2Event event;
                    try {
                        event = ringBuffer.get(sequence);
                        event.setSql1(insertOrdersSqls);
                        event.setSql2(insertLineitemSqls);
                    } finally {
                        context.getEmittedDataCounter().getAndIncrement();
                        ringBuffer.publish(sequence);
                    }
                }
                logger.info("TPC-H insert producer has finished, round: {}", generator.getRound());
            } catch (Exception e) {
                logger.error("TPC-H insert producer failed, round: {}, due to: {}", generator.getRound(),
                    e.getMessage(), e);
                context.setException(e);
            } finally {
                context.getCountDownLatch().countDown();
                generator.close();
            }
        });
    }
}
