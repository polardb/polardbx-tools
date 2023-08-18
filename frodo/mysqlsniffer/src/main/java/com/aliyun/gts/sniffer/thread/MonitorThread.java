package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.thread.ConsumerThread;
import com.mysql.cj.jdbc.ConnectionGroup;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class MonitorThread extends Thread{
    private Collection<ConsumerThread> consumerThreadList=null;
    protected volatile boolean isRunning=true;
    private AbstractCaptureThread captureThread=null;
    public MonitorThread(List<ConsumerThread> consumerThreadList) {
        this.consumerThreadList=consumerThreadList;
    }


    public MonitorThread(List<ConsumerThread> consumerThreadList,AbstractCaptureThread abstractCaptureThread) {
        this.consumerThreadList=consumerThreadList;
        this.captureThread=abstractCaptureThread;
    }

    public void run(){
        statSqlConsumer();
    }
    public void statSqlConsumer(){
        long lastRequests=0l;
        //失败执行数
        long lastErrors=0l;
        long lastRTSum=0l;
        while (isRunning){
            try{
                Thread.sleep(Config.interval*1000);
            }catch (InterruptedException ex){
                return;
            }

            //总执行数
            long requests=0l;
            //失败执行数
            long errors=0l;
            long avgRT=0l;
            long avgRequest=0l;
            long avgError=0l;
            long rtSum=0l;

            for(ConsumerThread thread:consumerThreadList){
                requests+=thread.getRequests();
                errors+=thread.getErrors();
                rtSum+=thread.getRtSum();
            }
            long successDiff=(requests-errors)-(lastRequests-lastErrors);
            avgRT=(successDiff==0?1:(rtSum-lastRTSum)/successDiff);
            avgError=(errors-lastErrors)/Config.interval;
            avgRequest=(requests-lastRequests)/Config.interval;
            String msg=String.format("requests:%d\t errors:%d\t [request/s:%d\t error/s:%d]\t avgRT(us):%d]"
                    ,requests,errors,avgRequest,avgError,avgRT);
            lastErrors=errors;
            lastRequests=requests;
            lastRTSum=rtSum;
            System.out.println(msg);
        }
    }

    public void statGLCapture(){
        long lastEventCount=0l;
        while (isRunning){
            try{
                Thread.sleep(Config.interval*1000);
            }catch (InterruptedException ex){
                return;
            }
            long curEventCount=captureThread.getEventCount();
            System.out.println((curEventCount-lastEventCount)/Config.interval);
            lastEventCount=curEventCount;
        }
    }

}
