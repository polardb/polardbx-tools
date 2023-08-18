package com.aliyun.gts.sniffer.thread;


public abstract class AbstractCaptureThread extends Thread{
    protected int shardId;

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public volatile boolean running=true;
    public volatile boolean closed=false;
    protected  long eventCount=0l;
    public long getEventCount() {
        return eventCount;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }
}
