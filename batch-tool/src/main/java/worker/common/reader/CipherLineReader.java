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
import model.encrypt.BaseCipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;
import worker.common.BatchLineEvent;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CipherLineReader extends FileBufferedBatchReader {
    private static final Logger logger = LoggerFactory.getLogger(CipherLineReader.class);

    private final BaseCipher cipher;
    private final BufferedInputStream inputStream;
    private final ByteBuffer byteBuffer;

    public CipherLineReader(ProducerExecutionContext context,
                            List<File> fileList, int fileIndex,
                            BaseCipher cipher,
                            RingBuffer<BatchLineEvent> ringBuffer) {
        super(context, fileList, ringBuffer);
        this.cipher = cipher;
        this.localProcessingFileIndex = fileIndex;
        this.byteBuffer = ByteBuffer.allocate(4);
        try {
            this.inputStream = new BufferedInputStream(new FileInputStream(fileList.get(localProcessingFileIndex).getPath()));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    protected void readData() {
        try {
            String line;
            if (context.isWithHeader()) {
                readLine();
            }
            while ((line = readLine()) != null) {
                appendToLineBuffer(line);
            }
            emitLineBuffer();
            logger.info("{} 读取完毕，读取行数：{}", fileList.get(localProcessingFileIndex).getPath(),
                currentFileLineCount.get());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            IOUtil.close(inputStream);
        }
    }

    private String readLine() throws IOException {
        int i = inputStream.read();
        if (i == -1) {
            return null;
        }
        byte b1 = (byte) i;
        int len;
        if (b1 >= 0) {
            // short length
            byteBuffer.put(b1).put(readByte());
            byteBuffer.flip();
            len = byteBuffer.getShort();
        } else {
            // int length
            byteBuffer.put(b1).put(readByte()).put(readByte()).put(readByte());
            byteBuffer.flip();
            len = byteBuffer.getInt();
        }
        byteBuffer.clear();
        byte[] data = new byte[len];
        i = inputStream.read(data);
        if (i == -1) {
            throw new IllegalStateException("Expect more data in current state");
        }
        byte[] decryptedData;
        try {
            decryptedData = cipher.decrypt(data);
        } catch (Exception e) {
            logger.error("Failed to decrypted file {}: {}",
                fileList.get(localProcessingFileIndex).getName(), e.getMessage());
            throw new RuntimeException(e);
        }
        // todo charset
        return new String(decryptedData);
    }

    private byte readByte() throws IOException {
        int i = inputStream.read();
        if (i == -1) {
            throw new IllegalStateException("Expect more data in current state");
        }
        return (byte) i;
    }

    @Override
    protected void beforePublish() {
        context.getEmittedDataCounter().getAndIncrement();
    }

    @Override
    protected void close() {
        IOUtil.close(inputStream);
    }
}

