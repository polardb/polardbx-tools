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

package worker.common.reader;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.read.listener.ReadListener;
import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;
import worker.common.BatchLineEvent;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class XlsxReader extends FileBufferedBatchReader {
    private static final Logger logger = LoggerFactory.getLogger(XlsxReader.class);

    private final InputStream inputStream;

    public XlsxReader(ProducerExecutionContext context,
                      List<File> fileList, int fileIndex,
                      RingBuffer<BatchLineEvent> ringBuffer) {
        super(context, fileList, ringBuffer);
        this.localProcessingFileIndex = fileIndex;

        String filePath = fileList.get(fileIndex).getAbsolutePath();
        try {
            inputStream = new BufferedInputStream(new FileInputStream(filePath));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    protected void readData() {
        ReadListener<Map<Integer, String >> listener = new AnalysisEventListener<Map<Integer, String>>() {
            @Override
            public void invokeHeadMap(Map<Integer, String> map, AnalysisContext analysisContext) {
                if (context.isWithHeader()) {
                    return;
                }
                appendData(map.values());
            }

            @Override
            public void invoke(Map<Integer, String> map, AnalysisContext analysisContext) {
                appendData(map.values());
            }

            private void appendData(Collection<String> values) {
                localProcessingBlockIndex++;
                String line = String.join(ConfigConstant.MAGIC_CSV_SEP, values);
                appendToLineBuffer(line);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                emitLineBuffer();
            }
        };

        EasyExcel.read(inputStream, listener).sheet().doRead();
        logger.info("{} 读取完毕", fileList.get(localProcessingFileIndex).getPath());
    }

    @Override
    protected void close() {
        IOUtil.close(inputStream);
    }

    @Override
    protected void beforePublish() {
        context.getEmittedDataCounter().getAndIncrement();
    }

    @Override
    public boolean useMagicSeparator() {
        return true;
    }
}
