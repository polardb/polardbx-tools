package model.config;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileRecord {
    private final String filePath;
    /**
     * 从第1行开始
     */
    private final int startLine;

    public FileRecord(String filePath) {
        this.filePath = filePath;
        this.startLine = 1;
    }

    public FileRecord(String filePath, int startLine) {
        if (startLine <= 0) {
            throw new IllegalArgumentException("Start line starts from 1");
        }
        this.filePath = filePath;
        this.startLine = startLine;
    }

    public static List<FileRecord> fromFilePaths(List<String> filePaths) {
        return filePaths.stream().map(FileRecord::new)
            .collect(Collectors.toList());
    }

    public String getFilePath() {
        return filePath;
    }

    public int getStartLine() {
        return startLine;
    }
}
