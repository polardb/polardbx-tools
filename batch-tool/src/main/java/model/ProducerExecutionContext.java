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

package model;

import model.config.BaseConfig;
import model.config.ConfigConstant;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 读取文件的工作线程上下文
 */
public class ProducerExecutionContext extends BaseConfig {

    private ThreadPoolExecutor producerExecutor;

    private List<String> filePathList;

    private int parallelism;

    /**
     * in MB
     */
    private int readBlockSizeInMb = 2;

    private List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter;

    private int nextFileIndex = 0;
    private long nextBlockIndex = 0;

    private String contextString;

    private String historyFile;

    private AtomicInteger emittedDataCounter;

    private CountDownLatch countDownLatch;

    public ProducerExecutionContext() {
        super(ConfigConstant.DEFAULT_IMPORT_SHARDING_ENABLED);
    }

    public ThreadPoolExecutor getProducerExecutor() {
        return producerExecutor;
    }

    public void setProducerExecutor(ThreadPoolExecutor producerExecutor) {
        this.producerExecutor = producerExecutor;
    }

    public List<String> getFilePathList() {
        return filePathList;
    }

    public void setFilePathList(List<String> filePathList) {
        this.filePathList = filePathList;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getReadBlockSizeInMb() {
        return readBlockSizeInMb;
    }

    public void setReadBlockSizeInMb(int readBlockSizeInMb) {
        this.readBlockSizeInMb = readBlockSizeInMb;
    }

    public List<ConcurrentHashMap<Long, AtomicInteger>> getEventCounter() {
        return eventCounter;
    }

    public void setEventCounter(List<ConcurrentHashMap<Long, AtomicInteger>> eventCounter) {
        this.eventCounter = eventCounter;
    }

    public AtomicInteger getEmittedDataCounter() {
        return emittedDataCounter;
    }

    public void setEmittedDataCounter(AtomicInteger emittedDataCounter) {
        this.emittedDataCounter = emittedDataCounter;
    }

    public int getNextFileIndex() {
        return nextFileIndex;
    }

    public void setNextFileIndex(int nextFileIndex) {
        this.nextFileIndex = nextFileIndex;
    }

    public long getNextBlockIndex() {
        return nextBlockIndex;
    }

    public void setNextBlockIndex(long nextBlockIndex) {
        this.nextBlockIndex = nextBlockIndex;
    }

    public String getContextString() {
        return contextString;
    }

    public void setContextString(String contextString) {
        this.contextString = contextString;
    }

    public void checkAndSetContextString(String newContextString) {
        if (!StringUtils.equals(contextString, newContextString)) {
            setContextString(newContextString);
            setNextFileIndex(0);
            setNextBlockIndex(0);
        }
    }

    public String getHistoryFile() {
        return historyFile;
    }

    public void setHistoryFile(String historyFile) {
        this.historyFile = historyFile;
    }

    /**
     * TODO to be refactored
     */
    public void setHistoryFileAndParse(String historyFile) {
        setHistoryFile(historyFile);
        File file = new File(historyFile);
        Scanner fromFile = null;
        try {
            if (file.exists()) {
                fromFile = new Scanner(file);
                setContextString(fromFile.nextLine());
                setNextFileIndex(fromFile.nextInt());
                setNextBlockIndex(fromFile.nextLong());
            }
        } catch (IOException e) {
            fromFile.close();
            e.printStackTrace();
        }
    }

    public void saveToHistoryFile(boolean isFinished) {
        if (historyFile == null) {
            return;
        }
        try {
            File file = new File(historyFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(historyFile, false);
            BufferedWriter out = new BufferedWriter(fileWriter);
            if (isFinished) {
                nextFileIndex = filePathList.size();
                nextBlockIndex = 0;
            }
            out.write(contextString);
            out.newLine();
            out.write(Integer.toString(nextFileIndex));
            out.newLine();
            out.write(Long.toString(nextBlockIndex));
            out.newLine();
            out.flush();
            out.close();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public String toString() {
        return "ProducerExecutionContext{" +
            "filePathList=" + filePathList +
            ", parallelism=" + parallelism +
            ", readBlockSizeInMb=" + readBlockSizeInMb +
            ", " + super.toString() +
            '}';
    }
}
