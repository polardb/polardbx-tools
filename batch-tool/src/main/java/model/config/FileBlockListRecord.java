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

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FileBlockListRecord {

    private final List<File> fileList;

    private final AtomicInteger currentFileIndex;

    /**
     * 当前正在处理的文件序号
     */
    private final AtomicBoolean[] fileDoneList;
    /**
     * 每个文件已处理的block序号
     */
    private final AtomicLong[] startPosArr;

    public FileBlockListRecord(List<File> fileList, int nextFileIndex, long nextBlockIndex) {
        this.fileList = fileList;
        this.currentFileIndex = new AtomicInteger(nextFileIndex);
        this.fileDoneList = new AtomicBoolean[fileList.size()];
        this.startPosArr = new AtomicLong[fileList.size()];
        for (int i = 0; i < startPosArr.length; i++) {
            if (i < nextFileIndex) {
                fileDoneList[i] = new AtomicBoolean(true);
                startPosArr[i] = new AtomicLong(0);
            } else if (i == nextFileIndex) {
                fileDoneList[i] = new AtomicBoolean(false);
                startPosArr[i] = new AtomicLong(nextBlockIndex);
            } else {
                fileDoneList[i] = new AtomicBoolean(false);
                startPosArr[i] = new AtomicLong(0);
            }
        }
    }

    public AtomicInteger getCurrentFileIndex() {
        return currentFileIndex;
    }

    public AtomicBoolean[] getFileDoneList() {
        return fileDoneList;
    }

    public AtomicLong[] getStartPosArr() {
        return startPosArr;
    }

    public List<File> getFileList() {
        return fileList;
    }
}
