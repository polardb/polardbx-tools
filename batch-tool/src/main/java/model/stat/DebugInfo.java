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

package model.stat;

import com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DebugInfo {

    private final List<SqlStat> sqlStatList = new ArrayList<>();
    private final List<FileReaderStat> fileReaderStatList = new ArrayList<>();
    private RingBuffer ringBuffer;
    private CountDownLatch countDownLatch;
    private AtomicInteger remainDataCounter;

    public List<FileReaderStat> getFileReaderStatList() {
        return fileReaderStatList;
    }

    public List<SqlStat> getSqlStatList() {
        return sqlStatList;
    }

    public void addFileReaderStat(FileReaderStat fileReaderStat) {
        this.fileReaderStatList.add(fileReaderStat);
    }

    public void addSqlStat(SqlStat sqlStat) {
        this.sqlStatList.add(sqlStat);
    }

    public AtomicInteger getRemainDataCounter() {
        return remainDataCounter;
    }

    public void setRemainDataCounter(AtomicInteger remainDataCounter) {
        this.remainDataCounter = remainDataCounter;
    }

    public RingBuffer getRingBuffer() {
        return ringBuffer;
    }

    public void setRingBuffer(RingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }
}
