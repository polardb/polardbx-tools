package worker.common;

import model.config.CompressMode;
import util.FileUtil;
import util.IOUtil;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPOutputStream;

@NotThreadSafe
// TODO 待重构
public class NioFileWriter implements IFileWriter {

    private FileChannel appendChannel = null;
    private GZIPOutputStream gzipOutputStream = null;
    private final CompressMode compressMode;
    private boolean closed = false;

    public NioFileWriter(String fileName) {
        this(fileName, CompressMode.NONE);
    }

    public NioFileWriter(CompressMode compressMode) {
        this.compressMode = compressMode;
    }

    public NioFileWriter(String fileName, CompressMode compressMode) {
        this.compressMode = compressMode;
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
    public void close() {
        if (closed) {
            return;
        }
        closeCurFile();
        this.closed = false;
    }

    private void openFileChannel(String fileName) {
        try {
            this.appendChannel = FileUtil.createEmptyFileAndOpenChannel(fileName);
            if (compressMode == CompressMode.GZIP) {
                this.gzipOutputStream = IOUtil.createGzipOutputStream(appendChannel);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void writeNio(byte[] data) {
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
