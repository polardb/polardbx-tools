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

package worker.export;

import model.CyclicAtomicInteger;
import model.config.CompressMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;
import worker.common.writer.IFileWriter;
import worker.common.writer.NioFileWriter;
import worker.util.ExportUtil;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class CollectFragmentWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CollectFragmentWorker.class);

    private final Queue<ExportEvent> fragmentQueue;
    private final String[] filenames;
    private final CyclicAtomicInteger cyclicCounter;
    private final CountDownLatch fragmentCountLatch;
    private final CompressMode compressMode;
    private final Charset charset;

    private final Map<String, IFileWriter> fileWriterCache = new HashMap<>();

    public CollectFragmentWorker(Queue<ExportEvent> fragmentQueue, String[] filenames,
                                 CyclicAtomicInteger cyclicCounter, CountDownLatch fragmentCountLatch,
                                 CompressMode compressMode, Charset charset) {
        this.fragmentQueue = fragmentQueue;
        this.filenames = filenames;
        this.cyclicCounter = cyclicCounter;
        this.fragmentCountLatch = fragmentCountLatch;
        this.compressMode = compressMode;
        this.charset = charset;
    }

    @Override
    public void run() {
        try {
            while (!fragmentQueue.isEmpty()) {
                ExportEvent exportEvent = fragmentQueue.poll();
                byte[] data = exportEvent.getData();

                final String filename = ExportUtil.getFilename(filenames[cyclicCounter.next()], compressMode);
                ;

                IFileWriter fileWriter = fileWriterCache.computeIfAbsent(filename,
                    (key) -> new NioFileWriter(filename, compressMode, charset));
                fileWriter.write(data);
                logger.debug("向文件 {} 写入碎片数据 ", filename);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            for (IFileWriter fileWriter : fileWriterCache.values()) {
                IOUtil.close(fileWriter);
            }
            fragmentCountLatch.countDown();
        }
    }

}
