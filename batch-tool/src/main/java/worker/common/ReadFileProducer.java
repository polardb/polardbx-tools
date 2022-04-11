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

import com.google.common.base.Preconditions;
import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.FileLineRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileProducer.class);

    final ProducerExecutionContext context;
    final RingBuffer<BatchLineEvent> ringBuffer;

    protected final List<File> fileList;
    protected final List<FileLineRecord> fileLineRecordList;

    public ReadFileProducer(ProducerExecutionContext context,
                            RingBuffer<BatchLineEvent> ringBuffer,
                            List<FileLineRecord> fileLineRecordList) {
        this.context = context;
        this.ringBuffer = ringBuffer;
        Preconditions.checkArgument(!fileLineRecordList.isEmpty(), "No file for producer");
        this.fileLineRecordList = fileLineRecordList;
        this.fileList = new ArrayList<>(fileLineRecordList.size());
        initFileList();
    }

    public abstract void produce();

    /**
     * 初始化文件列表
     * 若有文件路径不存在 提前报错结束
     */
    private void initFileList() {
        for (FileLineRecord fileRecord : fileLineRecordList) {
            File file = new File(fileRecord.getFilePath());
            if (!file.exists()) {
                logger.error("File {} doesn't exist", fileRecord.getFilePath());
                throw new RuntimeException("File doesn't exist");
            }
            this.fileList.add(file);
        }
    }

    public boolean useMagicSeparator() {
        return false;
    }
}
