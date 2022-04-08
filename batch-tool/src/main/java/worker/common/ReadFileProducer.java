/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker.common;

import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.FileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileProducer.class);

    final ProducerExecutionContext context;
    final RingBuffer<BatchLineEvent> ringBuffer;

    protected final List<File> fileList;

    public ReadFileProducer(ProducerExecutionContext context,
                            RingBuffer<BatchLineEvent> ringBuffer,
                            String tableName) {
        this.context = context;
        this.ringBuffer = ringBuffer;
        List<FileRecord> allFilePathList = context.getFileRecordList();
        if (allFilePathList == null || allFilePathList.isEmpty()) {
            throw new IllegalArgumentException("File path list cannot be empty");
        }
        this.fileList = new ArrayList<>(allFilePathList.size());
        if (allFilePathList.size() == 1) {
            // 当只有一个文件时 无需匹配表名与文件名
            initFileList(allFilePathList.stream()
                .map(FileRecord::getFilePath).collect(Collectors.toList()));
            return;
        }
        // FIXME 文件名与表名的匹配判断
        List<String> filePathList = allFilePathList.stream()
            .map(FileRecord::getFilePath)
            .filter(filePath -> filePath.contains(tableName))
            .collect(Collectors.toList());
        if (filePathList.isEmpty()) {
            throw new IllegalArgumentException("No filename contains table: " + tableName);
        }
        initFileList(filePathList);
    }

    public abstract void produce();

    /**
     * 初始化文件列表
     * 若有文件路径不存在 提前报错结束
     */
    private void initFileList(List<String> filePathList) {
        for (String path : filePathList) {
            File file = new File(path);
            if (!file.exists()) {
                logger.error("File {} doesn't exist", path);
                throw new RuntimeException("File doesn't exist");
            }
            this.fileList.add(file);
        }
    }

}
