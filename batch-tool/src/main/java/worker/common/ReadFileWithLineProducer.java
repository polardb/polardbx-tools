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

import com.google.common.base.Preconditions;
import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.EncryptionConfig;
import model.config.FileFormat;
import model.config.FileLineRecord;
import model.encrypt.BaseCipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.reader.CipherLineReader;
import worker.common.reader.CsvReader;
import worker.common.reader.FileBufferedBatchReader;
import worker.common.reader.XlsxReader;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 按csv标准按行处理csv文本文件
 * 解压场景先通过解压文件来进行兜底处理
 * TODO 再套一层producer
 */
public class ReadFileWithLineProducer extends ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileWithLineProducer.class);

    private boolean useMagicSeparator = false;
    private final FileBufferedBatchReader[] fileReaders;

    public ReadFileWithLineProducer(ProducerExecutionContext context,
                                    RingBuffer<BatchLineEvent> ringBuffer,
                                    List<FileLineRecord> fileLineRecordList) {
        super(context, ringBuffer, fileLineRecordList);
        Preconditions.checkArgument(!fileLineRecordList.isEmpty(), "File record is empty");
        this.fileReaders = new FileBufferedBatchReader[fileLineRecordList.size()];
        initFileReaders();
    }

    private void initFileReaders() {
        for (int i = 0; i < fileReaders.length; i++) {
            fileReaders[i] = initFileReader(i);
        }
    }

    @Override
    public void produce() {
        // 并行度大小为文件数量
        // todo 暂时与文件数量相同 如果文件数量太多将控制并发度
        ThreadPoolExecutor threadPool = context.getProducerExecutor();

        for (FileBufferedBatchReader reader : fileReaders) {
            threadPool.submit(reader);
        }
    }

    private FileBufferedBatchReader initFileReader(int workerIndex) {
        FileFormat fileFormat = context.getFileFormat();
        switch (fileFormat) {
        case XLSX:
        case XLS:
        case ET:
            this.useMagicSeparator = true;
            return new XlsxReader(context, fileList, workerIndex, ringBuffer);
        case CSV:
        case LOG:
        case TXT:
            this.useMagicSeparator = true;
            return new CsvReader(context, fileList, workerIndex, ringBuffer);
        case NONE:
            // do nothing
            break;
        default:
            throw new UnsupportedOperationException("Unknown file format type: " + fileFormat);
        }
        if (!context.getEncryptionConfig().getEncryptionMode().isSupportStreamingBit()) {
            BaseCipher cipher = BaseCipher.getCipher(context.getEncryptionConfig(), false);
            return new CipherLineReader(context, fileList, workerIndex, cipher, ringBuffer);
        }
        if (context.getEncryptionConfig().equals(EncryptionConfig.NONE)) {
            return new CsvReader(context, fileList, workerIndex, ringBuffer);
        }
        throw new IllegalArgumentException("Should use BlockReader in streaming bit encryption");
    }

    @Override
    public boolean useMagicSeparator() {
        return this.useMagicSeparator;
    }
}
