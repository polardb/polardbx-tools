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
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import model.ProducerExecutionContext;
import model.config.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 按csv标准按行处理csv文本文件
 */
public class ReadFileWithLineProducer extends ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileWithLineProducer.class);
    private final char sepChar;

    public ReadFileWithLineProducer(ProducerExecutionContext context, RingBuffer<BatchLineEvent> ringBuffer) {
        super(context, ringBuffer);
        String sep = context.getSep();
        if (sep.length() != 1) {
            logger.error("In quote escape mode only allows single char separator");
            System.exit(1);
        }
        this.sepChar = sep.charAt(0);
    }

    @Override
    public void produce() {
        // 并行度大小为文件数量
        // todo 暂时与文件数量相同 如果文件数量太多将控制并发度
        ThreadPoolExecutor threadPool = context.getProducerExecutor();
        CsvLineReader readFileWorker = null;
        try {
            for (int i = 0; i < fileList.size(); i++) {
                readFileWorker = new CsvLineReader(ringBuffer, i);
                threadPool.submit(readFileWorker);
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    class CsvLineReader extends ReadFileWorker {

        private final CSVReader reader;

        public CsvLineReader(RingBuffer<BatchLineEvent> ringBuffer, int fileIndex) throws FileNotFoundException {
            super(ringBuffer);

            CSVParser parser = new CSVParserBuilder().withSeparator(sepChar).build();
            this.reader = new CSVReaderBuilder(new InputStreamReader(
                new FileInputStream(fileList.get(fileIndex).getAbsolutePath()), context.getCharset()))
                .withCSVParser(parser).build();
            this.localProcessingFileIndex = fileIndex;
        }

        @Override
        public void run() {
            try {
                for (String[] fields; (fields = reader.readNext()) != null; ) {
                    localProcessingBlockIndex++;
                    String line = String.join(ConfigConstant.MAGIC_CSV_SEP, fields);
                    appendToLineBuffer(line);
                }

                reader.close();
                emitLineBuffer();
                logger.info("{} 读取完毕", fileList.get(localProcessingFileIndex).getPath());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                context.getCountDownLatch().countDown();
            }
        }

        @Override
        protected void beforePublish() {
            context.getEmittedDataCounter().getAndIncrement();
        }
    }
}
