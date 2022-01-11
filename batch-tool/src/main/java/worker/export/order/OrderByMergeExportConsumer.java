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
import worker.util.ExportUtil;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrderByMergeExportConsumer extends MergeExportConsumer {

    /**
     * 已经有序的队列数组
     * 等待merge输出
     */
    private final LinkedBlockingQueue[] orderedQueues;
    /**
     * 记录每个分片是否发送完成
     */
    private final AtomicBoolean[] finishedList;

    private final PriorityQueue<OrderByExportEvent> priorityQueue;

    public OrderByMergeExportConsumer(String filePath, String separator,
                                      List<FieldMetaInfo> orderByColumnInfoList, LinkedBlockingQueue[] orderedQueues,
                                      AtomicBoolean[] finishedList, int maxLine) {
        super(orderByColumnInfoList, maxLine, filePath, separator.getBytes());
        this.orderedQueues = orderedQueues;
        this.finishedList = finishedList;

        comparator = ExportUtil.getCombinedComparator(orderByColumnInfoList);

        if (!isAscending) {
            comparator = comparator.reversed();
        }
        priorityQueue = new PriorityQueue<>(orderedQueues.length, comparator);
    }

    public void consume() throws InterruptedException {
        OrderByExportEvent prioriElement, orderByExportEvent;
        // 等待所有队列都有数据
        for (int i = 0; i < orderedQueues.length; i++) {
            if (!finishedList[i].get() || !orderedQueues[i].isEmpty()) {
                // 未发送完成 或者 队列不为空 就阻塞获取
                // 因为设置 finished 标志后写入空消息, 这里不会阻塞
                orderByExportEvent = (OrderByExportEvent) orderedQueues[i].take();
                if (orderByExportEvent == null || orderByExportEvent.isEmpty()) {
                    // 说明该分片没有数据
                    continue;
                }
                priorityQueue.offer(orderByExportEvent);
            }
        }

        if (priorityQueue.isEmpty()) {
            // 说明没有任何数据
            return;
        }

        // 找到最优先的
        while (true) {
            prioriElement = priorityQueue.poll();
            // 该分片已经写入完毕
            if (prioriElement == null) {
                break;
            }
            // 写入文件
            int queueIndex = prioriElement.getQueueIndex();
            try {
                writeToBuffer(prioriElement.getData());
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (!finishedList[queueIndex].get() || !orderedQueues[queueIndex].isEmpty()) {
                // 未发送完成 或者 队列不为空 就阻塞获取
                // 因为设置 finished 标志后写入空消息, 这里不会阻塞
                orderByExportEvent = (OrderByExportEvent) orderedQueues[queueIndex].take();
                if (orderByExportEvent == null || orderByExportEvent.isEmpty()) {
                    // 可能队列已经空了 但未发送完成的标志没来得及设置
                    continue;
                }
                priorityQueue.offer(orderByExportEvent);
            } else {
                // 该分片已经写入完毕
                if (priorityQueue.isEmpty()) {
                    break;
                }
            }
        }

        // 写入缓冲区剩余的
        try {
            writeToFile(outputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
