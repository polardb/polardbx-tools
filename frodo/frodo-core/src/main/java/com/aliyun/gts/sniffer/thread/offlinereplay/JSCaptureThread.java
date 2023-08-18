package com.aliyun.gts.sniffer.thread.offlinereplay;

import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.thread.AbstractCaptureThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.HashMap;

public class JSCaptureThread extends AbstractCaptureThread {
    private Logger logger= LoggerFactory.getLogger(JSCaptureThread.class);
    private long eventCount=0l;
    private String file=null;
    private HashMap<Integer, JSConsumerThread> jsConsumerThreadMap=null;
    public JSCaptureThread(String file ,HashMap<Integer, JSConsumerThread> jsConsumerThreadMap) {
        this.file=file;
        this.jsConsumerThreadMap=jsConsumerThreadMap;
    }

    public void run() {
        logger.info("reader thread "+Thread.currentThread().getName()+" start!");
        BufferedReader reader=null;
        try{
            reader=new BufferedReader(new FileReader(file));
            String line=null;
            while(running){
                line =reader.readLine();
                if(line!=null){
                    Integer curKey=(int)(eventCount % jsConsumerThreadMap.size());
                    //同步添加，如果队列满了，那么会等待消费队列空闲
                    jsConsumerThreadMap.get(curKey).add(line);
                    eventCount++;
                }
                //如果为null
                if(line==null){
                    reader.close();
                    if(Config.circle && running){
                        reader.close();
                        reader=new BufferedReader(new FileReader(file));
                        continue;
                    }
                    running=false;
                    //等待consumer线程跑完
                    while(true){
                        boolean waitBreak=true;
                        for (JSConsumerThread thread:jsConsumerThreadMap.values()){
                            if(thread.getQueueSize()>0){
                                waitBreak=false;
                            }
                        }
                        if(waitBreak){
                            break;
                        }
                        //如果其他线程依然存在未消费完的消息，那么等待一段时间
                        Thread.sleep(100);
                    }
                }
            }

        }catch (Exception e){
            logger.error("open json file failed",e);
            return;
        }
        closed=true;
        //通知消费线程停止
        for(JSConsumerThread thread:jsConsumerThreadMap.values()){
            thread.setRunning(false);
        }
        logger.info("reader thread "+Thread.currentThread().getName()+" closed!");
    }

}
