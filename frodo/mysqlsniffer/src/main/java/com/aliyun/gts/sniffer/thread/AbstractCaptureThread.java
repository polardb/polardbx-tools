package com.aliyun.gts.sniffer.thread;

/**
 * Created by zhaoke on 17/11/2.
 */
public abstract class AbstractCaptureThread extends Thread{
    public abstract  void close();
    protected  long eventCount=0l;
    public long getEventCount() {
        return eventCount;
    }
}
