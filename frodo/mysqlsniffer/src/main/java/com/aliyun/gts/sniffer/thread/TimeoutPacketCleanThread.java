package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.mypcap.MysqlMultiPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by zhaoke on 17/10/31.
 * 用于清理合包缓存中过期的包
 */
public class TimeoutPacketCleanThread extends  Thread {
    private Logger logger= LoggerFactory.getLogger(TimeoutPacketCleanThread.class);
    private long timeout=300000;//过期为30秒
    HashMap<Integer,LargePacketMergeThread> largePacketMergeThreadHashMap;
    private volatile boolean isRunning=true;
    public TimeoutPacketCleanThread(HashMap<Integer,LargePacketMergeThread> largePacketMergeThreadHashMap) {
        this.largePacketMergeThreadHashMap=largePacketMergeThreadHashMap;
    }
    private long largeTimeoutPacketCnt=0l;

    public long getLargeTimeoutPacketCnt() {
        return largeTimeoutPacketCnt;
    }

    @Override
    public void run() {
        while(isRunning){
            try{
                for(LargePacketMergeThread thread:largePacketMergeThreadHashMap.values()){
                    Iterator iter = thread.getLargePacketMap().entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry entry;
                        try{
                            entry = (Map.Entry) iter.next();
                            Long key = (Long)entry.getKey();
                            MysqlMultiPacket val = (MysqlMultiPacket)entry.getValue();
                            Date now =new Date();
                            if((now.getTime()-val.getUpdated().getTime())>timeout){
                                thread.getLargePacketMap().remove(key);
                                thread.largeDecr();
                                largeTimeoutPacketCnt++;
                            }
                        }catch (Exception e){
                            logger.error("清理过期packet失败",e);
                            continue;
                        }
                    }

                }
                Thread.sleep(1000);
            }catch (InterruptedException e){
                isRunning=false;
            }

        }

    }
    public void close(){
        this.isRunning=false;
    }



}
