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
import model.db.TableFieldMetaInfo;
import util.FileUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportConsumer implements WorkHandler<ExportEvent> {
    private final boolean isWithHeader;
    private final byte[] separator;
    private final TableFieldMetaInfo tableFieldMetaInfo;
    private FileChannel appendChannel = null;
    private final AtomicInteger emittedDataCounter;

    public ExportConsumer(String filename, AtomicInteger emittedDataCounter,
                          boolean isWithHeader, byte[] separator,
                          TableFieldMetaInfo tableFieldMetaInfo) {
        this.isWithHeader = isWithHeader;
        this.emittedDataCounter = emittedDataCounter;
        this.separator = separator;
        this.tableFieldMetaInfo = tableFieldMetaInfo;
        try {
            createNewFile(filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createNewFile(String tmpFileName) throws IOException {
        this.appendChannel = FileUtil.createEmptyFileAndOpenChannel(tmpFileName);
        if (isWithHeader) {
            appendHeader();
        }
    }

    private void appendHeader() throws IOException {
        byte[] header = FileUtil.getHeaderBytes(tableFieldMetaInfo.getFieldMetaInfoList(), separator);
        writeNio(header);
    }

    @Override
    public void onEvent(ExportEvent exportEvent) {
        writeNio(exportEvent.getData());
    }

    public void writeNio(byte[] data) {
        try {
            ByteBuffer src = ByteBuffer.wrap(data);
            int length;
            do {
                length = appendChannel.write(src);
            } while (length != 0);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            emittedDataCounter.getAndDecrement();
        }
    }

    public void close() {
        if (appendChannel != null) {
            try {
                appendChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
