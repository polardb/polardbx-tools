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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class CollectFragmentWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CollectFragmentWorker.class);

    private final Queue<ExportEvent> fragmentQueue;
    private final String[] filePaths;
    private final CyclicAtomicInteger cyclicCounter;
    private final CountDownLatch fragmentCountLatch;

    public CollectFragmentWorker(Queue<ExportEvent> fragmentQueue, String[] filePaths,
                                 CyclicAtomicInteger cyclicCounter, CountDownLatch fragmentCountLatch) {
        this.fragmentQueue = fragmentQueue;
        this.filePaths = filePaths;
        this.cyclicCounter = cyclicCounter;
        this.fragmentCountLatch = fragmentCountLatch;
    }

    @Override
    public void run() {
        while (!fragmentQueue.isEmpty()) {
            ExportEvent exportEvent = fragmentQueue.poll();
            byte[] data = exportEvent.getData();

            String filePath = filePaths[cyclicCounter.next()];
            try {
                FileChannel appendChannel = FileChannel.open(Paths.get(filePath),
                    StandardOpenOption.APPEND);
                IOUtil.writeNio(appendChannel, data);
                logger.debug("向文件 {} 写入碎片数据 ", filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        fragmentCountLatch.countDown();
    }

}
