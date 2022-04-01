package worker.common;

public interface IFileWriter {

    void nextFile(String fileName);

    void write(byte[] data);

    void close();
}
