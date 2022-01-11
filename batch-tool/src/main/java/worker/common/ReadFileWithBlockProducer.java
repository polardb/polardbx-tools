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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReadFileWithBlockProducer extends ReadFileProducer {
    private static final Logger logger = LoggerFactory.getLogger(ReadFileWithBlockProducer.class);

    /**
     * 当前正在处理的文件序号
     */
    private final AtomicInteger currentFileIndex;
    private final AtomicBoolean[] fileDoneList;
    /**
     * 每个文件已处理的block序号
     */
    private final AtomicLong[] startPosArr;

    /**
     * 默认2MB
     */
    private long readBlockSize;
    /**
     * 4KB
     */
    private static long READ_PADDING = 1024L * 4;

    public ReadFileWithBlockProducer(ProducerExecutionContext context,
                                     RingBuffer<BatchLineEvent> ringBuffer) {
        super(context, ringBuffer);
        this.readBlockSize = context.getReadBlockSizeInMb() * 1024L * 1024;
        this.currentFileIndex = new AtomicInteger(context.getNextFileIndex());
        List<String> filePathList = context.getFilePathList();
        if (filePathList == null || filePathList.isEmpty()) {
            throw new IllegalArgumentException("File path list cannot be empty");
        }
        if (currentFileIndex.get() >= filePathList.size()) {
            logger.warn("breakpoint in history_file says all tasks are finished, should not run it again "
                + "or you can delete/modify the history_file then retry");
            System.exit(1);
        }
        this.fileDoneList = new AtomicBoolean[filePathList.size()];
        this.startPosArr = new AtomicLong[filePathList.size()];
        for (int i = 0; i < startPosArr.length; i++) {
            if (i < context.getNextFileIndex()) {
                fileDoneList[i] = new AtomicBoolean(true);
                startPosArr[i] = new AtomicLong(0);
            } else if (i == context.getNextFileIndex()) {
                fileDoneList[i] = new AtomicBoolean(false);
                startPosArr[i] = new AtomicLong(context.getNextBlockIndex());
            } else {
                fileDoneList[i] = new AtomicBoolean(false);
                startPosArr[i] = new AtomicLong(0);
            }
        }
    }

    @Override
    public void produce() {
        int parallelism = context.getParallelism();
        ThreadPoolExecutor threadPool = context.getProducerExecutor();
        BlockerReader readFileWorker = null;
        try {
            for (int i = 0; i < parallelism; i++) {
                readFileWorker = new BlockerReader(ringBuffer);
                threadPool.submit(readFileWorker);
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    public AtomicBoolean[] getFileDoneList() {
        return fileDoneList;
    }

    class BlockerReader extends ReadFileWorker {

        private RandomAccessFile curRandomAccessFile;

        public BlockerReader(RingBuffer<BatchLineEvent> ringBuffer) throws FileNotFoundException {
            super(ringBuffer);

            // set localProcessingFileIndex and startPosArr[localProcessingFileIndex]
            this.localProcessingFileIndex = currentFileIndex.get();
            this.curRandomAccessFile = new RandomAccessFile(fileList.get(localProcessingFileIndex), "r");
        }

        public String rtrim(String s) {
            int len = s.length();
            while ((len > 0) && (s.charAt(len - 1) <= ' ')) {
                len--;
            }
            return (len < s.length()) ? s.substring(0, len) : s;
        }

        @Override
        public void run() {

            byte[] buffer = new byte[(int) (readBlockSize + READ_PADDING)];
            byte[] tmpByteBuffer;

            int curPos, curLen;
            while (true) {
                try {
                    localProcessingBlockIndex = startPosArr[localProcessingFileIndex].getAndIncrement();
                    long pos = localProcessingBlockIndex * readBlockSize;
                    // 首次进入该block，开始处理 : counter++
                    context.getEventCounter().get(localProcessingFileIndex)
                        .putIfAbsent(localProcessingBlockIndex, new AtomicInteger(0));
                    context.getEventCounter().get(localProcessingFileIndex)
                        .get(localProcessingBlockIndex).incrementAndGet();
                    // 跳过第一个换行符
                    boolean skipFirst = (pos != 0);
                    curRandomAccessFile.seek(pos);
                    int len = curRandomAccessFile.read(buffer);

                    if (len == -1) {
                        if (!nextFile()) {
                            // 没有再下一个要处理的文件了, 结束
                            break;
                        }
                        continue;
                    }
                    curPos = 0;
                    curLen = 0;
                    label_reading:
                    while (curPos + curLen < len) {
                        // 读取行
                        switch (buffer[curLen + curPos]) {
                        case '\n':
                            if (skipFirst) {
                                skipFirst = false;
                            } else if (!(curPos == 0 && context.isWithHeader())) {
                                if (curLen + curPos - 1 >= 0 && buffer[curLen + curPos - 1] == '\r') {
                                    tmpByteBuffer = new byte[curLen - 1];
                                    System.arraycopy(buffer, curPos, tmpByteBuffer, 0, curLen - 1);
                                } else {
                                    tmpByteBuffer = new byte[curLen];
                                    System.arraycopy(buffer, curPos, tmpByteBuffer, 0, curLen);
                                }
                                String line = new String(tmpByteBuffer, context.getCharset());
                                line = rtrim(line);
                                // Remove BOM if utf?.
                                if (!line.isEmpty() && line.charAt(0) == '\uFEFF' && context.getCharset().toString()
                                    .toLowerCase().contains("utf")) {
                                    line = line.substring(1);
                                }
                                appendToLineBuffer(line);
                            }

                            curPos = curLen + curPos + 1;
                            curLen = 0;
                            if (curPos + curLen > readBlockSize) {
                                // 到达了padding处 停止
                                break label_reading;
                            }
                            break;
                        default:
                            curLen++;
                        }
                    }
                    // Dealing last line without '\n'.
                    if (curPos + curLen == len && // Read till EOF.
                        curPos + curLen <= readBlockSize) { // And not in padding.
                        // Dealing last line.
                        if (curLen + curPos - 1 >= 0 && buffer[curLen + curPos - 1] == '\r') {
                            tmpByteBuffer = new byte[curLen - 1];
                            System.arraycopy(buffer, curPos, tmpByteBuffer, 0, curLen - 1);
                        } else {
                            tmpByteBuffer = new byte[curLen];
                            System.arraycopy(buffer, curPos, tmpByteBuffer, 0, curLen);
                        }
                        String line = new String(tmpByteBuffer, context.getCharset());
                        line = rtrim(line);
                        // Remove BOM if utf?.
                        if (!line.isEmpty() && line.charAt(0) == '\uFEFF' && context.getCharset().toString()
                            .toLowerCase().contains("utf")) {
                            line = line.substring(1);
                        }
                        appendToLineBuffer(line);
                    }
                    // 正常处理完本block数据 : counter--
                    context.getEventCounter().get(localProcessingFileIndex)
                        .get(localProcessingBlockIndex).getAndDecrement();
                } catch (EOFException e) {
                    if (!nextFile()) {
                        // 没有再下一个要处理的文件了, 结束
                        break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            // 发送剩余数据
            if (bufferedLineCount != 0) {
                emitLineBuffer();
            }
            context.getCountDownLatch().countDown();
        }

        private boolean nextFile() {
            if (fileDoneList[localProcessingFileIndex].compareAndSet(false, true)) {
                logger.info("{} 读取完毕", fileList.get(localProcessingFileIndex).getPath());
            }
            // 未处理足一个block就进入下一个文件 : counter--
            context.getEventCounter().get(localProcessingFileIndex)
                .get(localProcessingBlockIndex).getAndDecrement();
            // 进入下一个文件
            if (localProcessingFileIndex < fileList.size() - 1) {
                currentFileIndex.compareAndSet(localProcessingFileIndex, localProcessingFileIndex + 1);
                // 如果并发很大的话 可以考虑一次性跳过多个文件
                localProcessingFileIndex++;
                localProcessingBlockIndex = -1;
                try {
                    curRandomAccessFile = new RandomAccessFile(fileList.get(localProcessingFileIndex), "r");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                return true;
            }
            return false;
        }

        @Override
        protected void beforePublish() {
            context.getEmittedDataCounter().getAndIncrement();
            context.getEventCounter().get(localProcessingFileIndex).
                get(localProcessingBlockIndex).getAndIncrement();
        }
    }

}
