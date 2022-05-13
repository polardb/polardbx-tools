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
import model.ProducerExecutionContext;
import model.config.CompressMode;
import model.config.FileBlockListRecord;
import model.config.FileLineRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.reader.BlockReader;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadFileWithBlockProducer extends ReadFileProducer {
    private static final Logger logger = LoggerFactory.getLogger(ReadFileWithBlockProducer.class);

    private final CompressMode compressMode;
    private final FileBlockListRecord fileBlockListRecord;

    public ReadFileWithBlockProducer(ProducerExecutionContext context,
                                     RingBuffer<BatchLineEvent> ringBuffer,
                                     List<FileLineRecord> fileLineRecordList) {
        super(context, ringBuffer, fileLineRecordList);
        this.compressMode = context.getCompressMode();
        this.fileBlockListRecord = new FileBlockListRecord(fileList, context.getNextFileIndex(),
            context.getNextBlockIndex());
    }

    @Override
    public void produce() {
        int parallelism = context.getParallelism();
        ThreadPoolExecutor threadPool = context.getProducerExecutor();
        BlockReader readFileWorker = null;
        for (int i = 0; i < parallelism; i++) {
            readFileWorker = new BlockReader(context, fileBlockListRecord, ringBuffer, compressMode);
            threadPool.submit(readFileWorker);
        }
    }

    public AtomicBoolean[] getFileDoneList() {
        return fileBlockListRecord.getFileDoneList();
    }
}
