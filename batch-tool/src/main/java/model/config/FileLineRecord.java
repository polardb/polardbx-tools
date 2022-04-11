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

package model.config;

import java.util.List;
import java.util.stream.Collectors;

public class FileLineRecord {
    private final String filePath;
    /**
     * 从第1行开始
     */
    private final int startLine;

    public FileLineRecord(String filePath) {
        this.filePath = filePath;
        this.startLine = 1;
    }

    public FileLineRecord(String filePath, int startLine) {
        if (startLine <= 0) {
            throw new IllegalArgumentException("Start line starts from 1");
        }
        this.filePath = filePath;
        this.startLine = startLine;
    }

    public static List<FileLineRecord> fromFilePaths(List<String> filePaths) {
        return filePaths.stream().map(FileLineRecord::new)
            .collect(Collectors.toList());
    }

    public String getFilePath() {
        return filePath;
    }

    @Override
    public String toString() {
        return "FileLineRecord{" +
            "filePath='" + filePath + '\'' +
            ", startLine=" + startLine +
            '}';
    }

    public int getStartLine() {
        return startLine;
    }
}
