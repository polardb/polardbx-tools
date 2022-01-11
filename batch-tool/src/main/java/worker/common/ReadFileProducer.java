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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileProducer.class);

    final ProducerExecutionContext context;
    final RingBuffer<BatchLineEvent> ringBuffer;

    final List<File> fileList;

    public ReadFileProducer(ProducerExecutionContext context,
                            RingBuffer<BatchLineEvent> ringBuffer) {
        this.context = context;
        this.ringBuffer = ringBuffer;
        List<String> filePathList = context.getFilePathList();
        this.fileList = new ArrayList<>(filePathList.size());
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
                System.exit(1);
            }
            this.fileList.add(file);
        }
    }

}
