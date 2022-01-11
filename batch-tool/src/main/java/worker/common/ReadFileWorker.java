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

package worker.common;

import com.lmax.disruptor.RingBuffer;
import model.config.ConfigConstant;
import model.config.GlobalVar;

import static model.config.GlobalVar.EMIT_BATCH_SIZE;

public abstract class ReadFileWorker implements Runnable {

    protected final RingBuffer<BatchLineEvent> ringBuffer;
    protected int bufferedLineCount = 0;
    protected String[] lineBuffer;
    protected volatile int localProcessingFileIndex;
    protected long localProcessingBlockIndex = -1;

    protected ReadFileWorker(RingBuffer<BatchLineEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
        this.lineBuffer = new String[EMIT_BATCH_SIZE];
    }

    protected void appendToLineBuffer(String line) {
        lineBuffer[bufferedLineCount++] = line;
        if (bufferedLineCount == EMIT_BATCH_SIZE) {
            emitLineBuffer();
            lineBuffer = new String[EMIT_BATCH_SIZE];
            bufferedLineCount = 0;
        }
    }

    protected void emitLineBuffer() {
        long sequence = ringBuffer.next();
        BatchLineEvent event;
        try {
            event = ringBuffer.get(sequence);
            if (bufferedLineCount < EMIT_BATCH_SIZE) {
                // 插入结束标志
                lineBuffer[bufferedLineCount] = ConfigConstant.END_OF_BATCH_LINES;
            }
            event.setBatchLines(lineBuffer);
            event.setLocalProcessingFileIndex(localProcessingFileIndex);
            event.setLocalProcessingBlockIndex(localProcessingBlockIndex);
        } finally {
            beforePublish();
            ringBuffer.publish(sequence);
        }
    }

    protected abstract void beforePublish();
}
