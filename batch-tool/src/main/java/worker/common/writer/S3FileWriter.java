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

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import store.FileStorage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;

@NotThreadSafe
public class S3FileWriter implements IFileWriter {

    private final FileStorage fileStorage;
    private final IFileWriter fileWriter;
    private String fileName;

    public S3FileWriter(IFileWriter fileWriter, FileStorage fileStorage) {
        Preconditions.checkNotNull(fileWriter);
        Preconditions.checkNotNull(fileStorage);
        this.fileStorage = fileStorage;
        this.fileWriter = fileWriter;
    }

    @Override
    public void finishLastFile() {
        try {
            fileStorage.put(fileName, fileName);
        } finally {
            // delete local file even if exception occurs
            FileUtils.deleteQuietly(new File(fileName));
        }
    }

    @Override
    public void nextFile(String fileName) {
        this.fileName = fileName;
        fileWriter.nextFile(fileName);
    }

    @Override
    public void write(byte[] data) {
        fileWriter.write(data);
    }

    @Override
    public void writeLine(String[] values) {
        fileWriter.writeLine(values);
    }

    @Override
    public boolean produceByBlock() {
        return fileWriter.produceByBlock();
    }

    @Override
    public void close() {
        fileWriter.close();
    }

}
