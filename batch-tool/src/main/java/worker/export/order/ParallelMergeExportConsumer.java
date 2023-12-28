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

package worker.export.order;

import model.db.FieldMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.MyThreadPool;
import worker.util.ExportUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelMergeExportConsumer extends MergeExportConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ParallelMergeExportConsumer.class);

    /**
     * 已经有序的队列数组
     * 等待merge输出
     */
    private final LinkedList[] orderedLists;

    public ParallelMergeExportConsumer(String filePath, String separator,
                                       List<FieldMetaInfo> orderByColumnInfoList,
                                       LinkedList[] orderedLists,
                                       int maxLine) {
        super(orderByColumnInfoList, maxLine, filePath, separator.getBytes());
        this.orderedLists = orderedLists;
        this.comparator = ExportUtil.getCombinedParallelOrderComparator(orderByColumnInfoList);

        if (!isAscending) {
            comparator = comparator.reversed();
        }
    }

    public void consume() throws InterruptedException {
        // 对进行并行归并
        // 假定分片数是2的整次幂
        // 从目前的设计上看 只需要偶数就行
        // 但是暂时假定是2的整次幂
        int count = orderedLists.length;
        assert Integer.bitCount(count) == 1;

        LinkedBlockingDeque<LinkedList<ParallelOrderByExportEvent>> linkedListQueue =
            new LinkedBlockingDeque<>();

        ExecutorService executor = MyThreadPool.createExecutorWithEnsure("ParallelMergeExportConsumer",
            count);
        AtomicInteger runningThreadCount = new AtomicInteger(count / 2);

        logger.info("开始归并");

        for (int i = 0; i < count; i += 2) {
            MergeThread mergeThread = new MergeThread(orderedLists[i],
                orderedLists[i + 1], linkedListQueue, runningThreadCount);
            executor.submit(mergeThread);
        }

        // 最后归并到只有一个
        while (runningThreadCount.get() + linkedListQueue.size() >= 2) {
            logger.debug("当前队列大小 {}, 归并线程数 {} ", linkedListQueue.size(), runningThreadCount.get());
            LinkedList<ParallelOrderByExportEvent> list1 = linkedListQueue.take();
            logger.debug("Take后当前队列大小 {}, 归并线程数 {} ", linkedListQueue.size(), runningThreadCount.get());
            LinkedList<ParallelOrderByExportEvent> list2 = linkedListQueue.take();
            runningThreadCount.incrementAndGet();
            MergeThread mergeThread = new MergeThread(list1, list2,
                linkedListQueue, runningThreadCount);
            executor.submit(mergeThread);
        }

        logger.debug("归并结束，队列长度为{}，开始写入文件", linkedListQueue.size());
        // 确保最后会只剩一个
        LinkedList<ParallelOrderByExportEvent> finalResult = linkedListQueue.take();
        try {
            for (ParallelOrderByExportEvent parallelOrderByExportEvent : finalResult) {
                writeToBuffer(parallelOrderByExportEvent.getData());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("写入文件结束");
    }

    private class MergeThread implements Runnable {
        LinkedList<ParallelOrderByExportEvent> list1;
        LinkedList<ParallelOrderByExportEvent> list2;

        Queue<LinkedList<ParallelOrderByExportEvent>> linkedListQueue;
        AtomicInteger runningThreadCount;

        public MergeThread(LinkedList<ParallelOrderByExportEvent> list1,
                           LinkedList<ParallelOrderByExportEvent> list2,
                           Queue<LinkedList<ParallelOrderByExportEvent>> linkedListQueue,
                           AtomicInteger runningThreadCount) {
            this.list1 = list1;
            this.list2 = list2;
            this.linkedListQueue = linkedListQueue;
            this.runningThreadCount = runningThreadCount;
        }

        @Override
        public void run() {
            LinkedList<ParallelOrderByExportEvent> mergeResult = mergeTwoList(list1, list2);
            logger.info("{}归并完毕，放入队列中，此时归并线程数为 {}", Thread.currentThread().getName(), runningThreadCount.get());
            linkedListQueue.offer(mergeResult);
            runningThreadCount.getAndDecrement();
        }
    }

    private LinkedList<ParallelOrderByExportEvent> mergeTwoList(LinkedList<ParallelOrderByExportEvent> list1,
                                                                LinkedList<ParallelOrderByExportEvent> list2) {
        ListIterator<ParallelOrderByExportEvent> it1 = list1.listIterator();
        ListIterator<ParallelOrderByExportEvent> it2 = list2.listIterator();
        LinkedList<ParallelOrderByExportEvent> result = new LinkedList<>();
        while (it1.hasNext() || it2.hasNext()) {
            if (it1.hasNext() && it2.hasNext()) {
                ParallelOrderByExportEvent val0 = it1.next();
                ParallelOrderByExportEvent val2 = it2.next();
                if (comparator.compare(val0, val2) > 0) {
                    result.add(val0);
                    it2.previous();
                } else {
                    result.add(val2);
                    it1.previous();
                }
            } else if (!it1.hasNext()) {
                result.add(it2.next());
            } else {
                result.add(it1.next());
            }
        }
        return result;
    }

}
