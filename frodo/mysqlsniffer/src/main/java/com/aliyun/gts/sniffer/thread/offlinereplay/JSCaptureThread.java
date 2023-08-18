package com.aliyun.gts.sniffer.thread.offlinereplay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.core.MysqlSniffer;
import com.aliyun.gts.sniffer.thread.AbstractCaptureThread;
import com.aliyun.gts.sniffer.thread.generallog.GLConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JSCaptureThread extends AbstractCaptureThread {
    private Logger logger= LoggerFactory.getLogger(JSCaptureThread.class);
    private volatile boolean isRunning=true;
    private JDBCUtils jdbcUtils=null;
    private long eventCount=0l;
    private HashMap<Integer, JSConsumerThread> jsConsumerThreadMap=null;
    public JSCaptureThread(HashMap<Integer, JSConsumerThread> jsConsumerThreadMap) throws SQLException{
        String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql?allowPublicKeyRetrieval=true";
        jdbcUtils=new JDBCUtils(url,Config.getDstUsername(), Config.getDstPassword(),"mysql");
        this.jsConsumerThreadMap=jsConsumerThreadMap;
    }

    public void run() {
        BufferedReader reader=null;
        try{
            reader=new BufferedReader(new FileReader(Config.replayJSONFilePath));
            String line=null;
            while(isRunning ){
                if(eventCount%50000==0){
                    System.out.println(eventCount);
                }
                line =reader.readLine();
                if(line!=null){
                    Integer key=(int)(eventCount % Config.getSqlThreadCnt());
                    //同步添加，如果队列满了，那么会等待消费队列空闲
                    jsConsumerThreadMap.get(key).add(line);
                    eventCount++;
                }
                //如果为null
                if(line==null){
                    reader.close();
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
                    if(Config.circle){
                        reader.close();
                        reader=new BufferedReader(new FileReader(Config.replayJSONFilePath));
                    }else{
                        isRunning=false;
                        MysqlSniffer.stop();
                    }

                }
            }

        }catch (Exception e){
            logger.error("open general log failed",e);
            return;
        }
    }

    public void close(){
        isRunning=false;
    }



}
