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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.FileStorage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class ReadFileProducer implements Producer {

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
        FileStorage fileStorage = context.getFileStorage();
        if (fileStorage == null) {
            for (FileLineRecord fileRecord : fileLineRecordList) {
                File file = new File(fileRecord.getFilePath());
                if (!file.exists()) {
                    logger.error("File {} doesn't exist", fileRecord.getFilePath());
                    throw new RuntimeException("File doesn't exist");
                }
                this.fileList.add(file);
            }
        } else {
            // 预下载文件，先不做流水线
            ThreadPoolExecutor producerExecutor = context.getProducerExecutor();
            final CountDownLatch countDownLatch = new CountDownLatch(fileLineRecordList.size());
            for (FileLineRecord fileRecord : fileLineRecordList) {
                File file = new File(fileRecord.getFilePath());
                if (!file.exists()) {
                    producerExecutor.submit(() -> {
                        try {
                            fileStorage.get(file.getName(), file.getAbsolutePath());
                        } catch (Exception e) {
                            logger.error("Download file {} failed", fileRecord.getFilePath(), e);
                            throw new RuntimeException(e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
                }
                this.fileList.add(file);
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean useMagicSeparator() {
        return false;
    }

    public void close() {
        FileStorage fileStorage = context.getFileStorage();
        if (fileStorage != null) {
            // 在 producer 进行临时文件清理, 因为同一个文件可能被多个reader共享
            for (FileLineRecord fileRecord : fileLineRecordList) {
                File file = new File(fileRecord.getFilePath());
                FileUtils.deleteQuietly(file);
            }
        }
    }
}
