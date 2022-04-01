package util;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static model.config.ConfigConstant.DEFAULT_COMPRESS_BUFFER_SIZE;

public class IOUtil {

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
}
