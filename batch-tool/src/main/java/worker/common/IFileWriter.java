package worker.common;

public interface IFileWriter {

    void nextFile(String fileName);

    default void write(byte[] data) {
        throw new UnsupportedOperationException(getClass() + " does not support write raw bytes");
    }

    default void writeLine(String[] values) {
        throw new UnsupportedOperationException(getClass() + " does not support write line");
    }

    void close();
}
