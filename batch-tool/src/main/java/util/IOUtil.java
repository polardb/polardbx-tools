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

package util;

import org.apache.commons.io.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static model.config.ConfigConstant.DEFAULT_COMPRESS_BUFFER_SIZE;

public class IOUtil {

    public static FileChannel createEmptyFileAndOpenChannel(String tmpFileName) {
        File file = new File(tmpFileName);
        FileUtils.deleteQuietly(file);
        try {
            file.createNewFile();
            return FileChannel.open(Paths.get(tmpFileName), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void writeNio(FileChannel fileChannel, byte[] data) throws IOException {
        ByteBuffer src = ByteBuffer.wrap(data);
        int length = fileChannel.write(src);
        while (length != 0) {
            length = fileChannel.write(src);
        }
    }

    public static void close(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void finish(DeflaterOutputStream outputStream) {
        if (outputStream != null) {
            try {
                outputStream.finish();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(OutputStream outputStream) {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static GZIPOutputStream createGzipOutputStream(FileChannel channel) {
        try {
            return new GZIPOutputStream(Channels.newOutputStream(channel), DEFAULT_COMPRESS_BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void flush(OutputStream outputStream) {
        if (outputStream != null) {
            try {
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
