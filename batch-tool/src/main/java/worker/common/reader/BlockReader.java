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

import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.CompressMode;
import model.config.ConfigConstant;
import model.config.FileBlockListRecord;
import model.encrypt.BaseCipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BatchLineEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class BlockReader extends FileBufferedBatchReader {
    private static final Logger logger = LoggerFactory.getLogger(BlockReader.class);

    private RandomAccessFile curRandomAccessFile;

    /**
     * 默认2MB
     */
    private long readBlockSize;
    /**
     * 4KB
     */
    private static long READ_PADDING = 1024L * 4;
    private final BaseCipher cipher;
    private final FileBlockListRecord fileBlockListRecord;

    public BlockReader(ProducerExecutionContext context,
                       FileBlockListRecord fileBlockListRecord,
                       RingBuffer<BatchLineEvent> ringBuffer, CompressMode compressMode)  {
        super(context, fileBlockListRecord.getFileList(), ringBuffer, compressMode);
        this.readBlockSize = context.getReadBlockSizeInMb() * 1024L * 1024;
        // set localProcessingFileIndex and startPosArr[localProcessingFileIndex]
        this.localProcessingFileIndex = fileBlockListRecord.getCurrentFileIndex().get();
        this.fileBlockListRecord = fileBlockListRecord;
        try {
            this.curRandomAccessFile = new RandomAccessFile(fileList.get(localProcessingFileIndex), "r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.cipher = BaseCipher.getCipher(context.getEncryptionConfig(), false);
    }

    public String rtrim(String s) {
        int len = s.length();
        while ((len > 0) && (s.charAt(len - 1) <= ' ')) {
            len--;
        }
        return (len < s.length()) ? s.substring(0, len) : s;
    }


    @Override
    protected void readData() {
        byte[] buffer = new byte[(int) (readBlockSize + READ_PADDING)];
        byte[] tmpByteBuffer;

        int curPos, curLen;
        while (true) {
            try {
                localProcessingBlockIndex = fileBlockListRecord.getStartPosArr()[localProcessingFileIndex].getAndIncrement();
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
                // TODO 待重构
                if (this.compressMode == CompressMode.GZIP) {
                    // 将buffer的内容解压
                    GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(buffer),
                        ConfigConstant.DEFAULT_COMPRESS_BUFFER_SIZE);
                    ByteArrayOutputStream gzipOutputBuffer = new ByteArrayOutputStream(buffer.length * 2);
                    int num = 0;
                    byte[] gzipBuffer = new byte[buffer.length];
                    while ((num = gzipInputStream.read(gzipBuffer, 0, len)) != -1) {
                        gzipOutputBuffer.write(gzipBuffer, 0, num);
                    }
                    buffer = gzipOutputBuffer.toByteArray();
                    len = buffer.length;
                    gzipInputStream.close();
                }
                if (cipher != null) {
                    buffer = cipher.decrypt(buffer);
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
                logger.error(e.getMessage());
                break;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        // 发送剩余数据
        if (bufferedLineCount != 0) {
            emitLineBuffer();
        }
    }

    private boolean nextFile() {
        if (fileBlockListRecord.getFileDoneList()[localProcessingFileIndex].compareAndSet(false, true)) {
            logger.info("{} 读取完毕", fileList.get(localProcessingFileIndex).getPath());
        }
        // 未处理足一个block就进入下一个文件 : counter--
        context.getEventCounter().get(localProcessingFileIndex)
            .get(localProcessingBlockIndex).getAndDecrement();
        // 进入下一个文件
        if (localProcessingFileIndex < fileList.size() - 1) {
            fileBlockListRecord.getCurrentFileIndex().compareAndSet(localProcessingFileIndex, localProcessingFileIndex + 1);
            // 如果并发很大的话 可以考虑一次性跳过多个文件
            localProcessingFileIndex++;
            localProcessingBlockIndex = -1;
            try {
                curRandomAccessFile = new RandomAccessFile(fileList.get(localProcessingFileIndex), "r");
            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
                return false;
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
