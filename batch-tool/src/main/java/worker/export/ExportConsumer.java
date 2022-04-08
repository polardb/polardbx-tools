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

package worker.export;

import com.lmax.disruptor.WorkHandler;
import model.config.CompressMode;
import model.db.TableFieldMetaInfo;
import model.encrypt.Cipher;
import util.FileUtil;
import worker.common.IFileWriter;
import worker.common.NioFileWriter;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportConsumer implements WorkHandler<ExportEvent> {
    private final boolean isWithHeader;
    private final byte[] separator;
    private final TableFieldMetaInfo tableFieldMetaInfo;
    private final IFileWriter fileWriter;
    private final AtomicInteger emittedDataCounter;

    private Cipher cipher = null;

    public ExportConsumer(String filename, AtomicInteger emittedDataCounter,
                          boolean isWithHeader, byte[] separator,
                          TableFieldMetaInfo tableFieldMetaInfo,
                          CompressMode compressMode, Charset charset) {
        this.isWithHeader = isWithHeader;
        this.emittedDataCounter = emittedDataCounter;
        this.separator = separator;
        this.tableFieldMetaInfo = tableFieldMetaInfo;
        this.fileWriter = new NioFileWriter(filename, compressMode, charset);
        if (isWithHeader) {
            appendHeader();
        }
    }

    private void appendHeader() {
        byte[] header = FileUtil.getHeaderBytes(tableFieldMetaInfo.getFieldMetaInfoList(), separator);
        fileWriter.write(header);
    }

    @Override
    public void onEvent(ExportEvent exportEvent) {
        writeEvent(exportEvent.getData());
    }

    public void writeEvent(byte[] data) {
        if (cipher != null) {
            try {
                data = cipher.encrypt(data);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        try {
            fileWriter.write(data);
        } finally {
            emittedDataCounter.getAndDecrement();
        }
    }

    public void setCipher(Cipher cipher) {
        this.cipher = cipher;
    }

    public void close() {
        fileWriter.close();
    }
}
