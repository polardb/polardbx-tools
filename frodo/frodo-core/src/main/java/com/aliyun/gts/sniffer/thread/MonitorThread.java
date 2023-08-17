package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.common.utils.SQLiteWrapper;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.core.Frodo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class MonitorThread extends Thread {
    private final Collection<ConsumerThread> consumerThreadList;
    private static final Logger logger = LoggerFactory.getLogger(MonitorThread.class);
    protected volatile boolean isRunning = true;
    private final AbstractCaptureThread captureThread = null;
    private SQLiteWrapper sqlLiteWrapper = null;

    public MonitorThread(List<ConsumerThread> consumerThreadList) {
        this.consumerThreadList = consumerThreadList;
        try {
            if (Config.sqlLitePath != null) {
                sqlLiteWrapper = new SQLiteWrapper(Config.sqlLitePath);
            }
        } catch (Exception e) {
            logger.error("init sqllite failed", e);
            sqlLiteWrapper = null;
        }
    }

    public void run() {
        statSqlConsumer();
    }

    public void statSqlConsumer() {
        long lastRequests = 0L;
        long lastRequestSkip = 0L;
        //失败执行数
        long lastErrors = 0L;
        long lastRTSum = 0L;
        long totalDelay;
        long maxDelay;
        long avgDelay;
        long minDelay;
        while (isRunning) {
            totalDelay = 0L;
            minDelay = 0L;
            maxDelay = 0L;
            try {
                Thread.sleep(Config.interval * 1000L);
            } catch (InterruptedException ex) {
                return;
            }
            //总执行数
            long requests = 0L;
            //总跳过行数
            long requestSkip = 0L;
            //失败执行数
            long errors = 0L;
            double avgRT;
            double avgRequest;
            double avgError;
            long rtSum = 0L;

            for (ConsumerThread thread : consumerThreadList) {
                requests += thread.getRequests();
                errors += thread.getErrors();
                rtSum += thread.getRtSum();
                requestSkip += thread.getSkipRequestCnt();
                totalDelay += thread.getDelay();
                if (maxDelay < thread.getDelay()) {
                    maxDelay = thread.getDelay();
                }
                if (minDelay > thread.getDelay()) {
                    minDelay = thread.getDelay();
                }
//                System.out.println(thread.getDelay());
            }
            avgDelay = totalDelay / consumerThreadList.size();
            long successDiff = (requests - errors) - (lastRequests - lastErrors);
            avgRT = (successDiff == 0 ? 1 : (rtSum - lastRTSum) / (double) successDiff);
            avgError = (errors - lastErrors) / (double) Config.interval;
            avgRequest = (requests - lastRequests) / (double) Config.interval;

            lastErrors = errors;
            lastRequests = requests;
            lastRTSum = rtSum;
            updateProgress(Frodo.readCnt, requests, errors, requestSkip, avgRequest, avgError, avgRT, maxDelay, minDelay, avgDelay);
        }
    }

    private void updateProgress(Long total, long requests, long errors, long skip, double requestPerSecond, double errorPerSecond, double avgRT, long maxDelay, long minDelay, long avgDelay) {
        double progress = 100 * (double) (requests + skip) / (double) (total == 0L ? 1L : total);
        String msg = String.format("progress:%.2f%%\t total:%d\t requests:%d\t errors:%d\t skip:%d\t [request/s:%.2f\t error/s:%.2f]\t avgRT(us):%.2f]\t delay:[max:%d\t min:%d\t avg:%d]"
                , progress, total, requests, errors, skip, requestPerSecond, errorPerSecond, avgRT, maxDelay, minDelay, avgDelay);
        System.out.println(msg);
        if (sqlLiteWrapper == null) {
            return;
        }
        String createSql = "CREATE TABLE IF NOT EXISTS frodo_progress (\n" +
                "task text not null,\n" +
                "total integer not null,\n" +
                "requests integer not null,\n" +
                "skip integer not null,\n" +
                "progress real not null,\n" +
                "errors integer not null,\n" +
                "request_per_second REAL not null,\n" +
                "error_per_second real not null,\n" +
                "avg_rt real not null,\n" +
                "PRIMARY KEY(task) \n" +
                ");";
        try {
            String replaceSqlFormat = "replace into frodo_progress(task,total,requests,skip,progress,errors,request_per_second,error_per_second,avg_rt)  values ('%s',%d,%d,%d,%.2f,%d,%.2f,%.2f,%.2f)";
            String sql = String.format(replaceSqlFormat, Config.task, total, requests, skip, progress, errors, requestPerSecond, errorPerSecond, avgRT);
            sqlLiteWrapper.execute(createSql);
            sqlLiteWrapper.execute(sql);

        } catch (Exception e) {
            logger.error("sqlite execute failed", e);
        }
    }

    public void close() {
        isRunning = false;
    }

}
