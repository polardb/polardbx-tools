package com.aliyun.gts.sniffer.thread;


public abstract class AbstractCaptureThread extends Thread{
    public abstract  void close();
    protected  long eventCount=0l;
    public long getEventCount() {
        return eventCount;
    }
}
