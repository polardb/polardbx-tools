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

import model.config.CompressMode;
import util.IOUtil;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

@NotThreadSafe
public class NioFileWriter implements IFileWriter {

    private FileChannel appendChannel = null;
    private GZIPOutputStream gzipOutputStream = null;
    private final CompressMode compressMode;
    private boolean closed = false;
    private final Charset charset;

    public NioFileWriter(String fileName) {
        this(fileName, CompressMode.NONE);
    }

    public NioFileWriter(CompressMode compressMode) {
        this(compressMode, StandardCharsets.UTF_8);
    }

    public NioFileWriter(CompressMode compressMode, Charset charset) {
        this.charset = charset;
        this.compressMode = compressMode;
    }

    public NioFileWriter(String fileName, CompressMode compressMode) {
        this(fileName, compressMode, StandardCharsets.UTF_8);
    }

    public NioFileWriter(String fileName, CompressMode compressMode, Charset charset) {
        this.compressMode = compressMode;
        this.charset = charset;
        openFileChannel(fileName);
    }

    @Override
    public void nextFile(String fileName) {
        closeCurFile();
        openFileChannel(fileName);
    }

    @Override
    public void write(byte[] data) {
        writeNio(data);
    }

    @Override
    public boolean produceByBlock() {
        return true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closeCurFile();
        this.closed = false;
    }

    private void openFileChannel(String fileName) {
        this.appendChannel = IOUtil.createEmptyFileAndOpenChannel(fileName);
        if (compressMode == CompressMode.GZIP) {
            this.gzipOutputStream = IOUtil.createGzipOutputStream(appendChannel);
        }
    }

    public void writeNio(byte[] data) {
        if (charset != StandardCharsets.UTF_8) {
            data = new String(data, StandardCharsets.UTF_8).getBytes(charset);
        }
        try {
            switch (compressMode) {
            case NONE:
                ByteBuffer src = ByteBuffer.wrap(data);
                int length = appendChannel.write(src);
                while (length != 0) {
                    length = appendChannel.write(src);
                }
                break;
            case GZIP:
                gzipOutputStream.write(data);
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void closeCurFile() {
        if (compressMode == CompressMode.NONE) {
            IOUtil.close(appendChannel);
        } else {
            IOUtil.finish(gzipOutputStream);
            IOUtil.close(gzipOutputStream);
        }
    }
}
