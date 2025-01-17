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

package worker.common.writer;

import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import model.encrypt.BaseCipher;
import util.FileUtil;
import util.IOUtil;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 基于按行加密的文件写入
 * 采用字符计数法
 */
public class CipherLineFileWriter implements IFileWriter {

    private FileChannel appendChannel = null;
    private final BaseCipher cipher;
    private final ByteBuffer byteBuffer;
    private final byte[] separator;
    private final QuoteEncloseMode quoteEncloseMode;

    public CipherLineFileWriter(BaseCipher cipher,
                                byte[] separator, QuoteEncloseMode quoteEncloseMode) {
        this.cipher = cipher;
        this.byteBuffer = ByteBuffer.allocateDirect(GlobalVar.DEFAULT_DIRECT_BUFFER_SIZE_PER_WORKER);
        this.separator = separator;
        this.quoteEncloseMode = quoteEncloseMode;
    }

    @Override
    public void nextFile(String fileName) {
        IOUtil.close(appendChannel);
        this.appendChannel = IOUtil.createEmptyFileAndOpenChannel(fileName);
    }

    @Override
    public void writeLine(String[] values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Values are empty");
        }
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream(values.length * 16);
            FileUtil.writeToByteArrayStream(os, values[0].getBytes());
            for (int i = 1; i < values.length; i++) {
                os.write(separator);
                FileUtil.writeToByteArrayStream(os, values[i].getBytes());
            }
            byte[] data = os.toByteArray();
            byte[] crypto = cipher.encrypt(data);
            // 使用2字节或者4字节表示长度
            int headerLen = crypto.length <= 0x0FFF ? 2 : 4;
            if (byteBuffer.remaining() < crypto.length + headerLen) {
                byteBuffer.flip();
                appendChannel.write(byteBuffer);
                byteBuffer.compact();
            }
            writeHeader(byteBuffer, crypto.length);
            byteBuffer.put(crypto);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private void writeHeader(ByteBuffer byteBuffer, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative");
        }
        if (length <= 0x0FFF) {
            byteBuffer.putShort((short) length);
            return;
        }
        byteBuffer.putInt(-length);
    }

    @Override
    public boolean produceByBlock() {
        return false;
    }

    @Override
    public void close() {
    }
}
