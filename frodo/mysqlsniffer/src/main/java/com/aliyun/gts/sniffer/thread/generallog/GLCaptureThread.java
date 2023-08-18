package com.aliyun.gts.sniffer.thread.generallog;

import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.common.utils.MysqlGLUtil;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.aliyun.gts.sniffer.thread.AbstractCaptureThread;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;


public class GLCaptureThread extends AbstractCaptureThread {
    private Logger logger= LoggerFactory.getLogger(GLCaptureThread.class);
    private volatile boolean isRunning=true;
    private JDBCUtils jdbcUtils;
    private HashMap<Integer, GLConsumerThread> glConsumerThreadMap=null;
    //processlist维护列表
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();
    public GLCaptureThread(JDBCUtils jdbcUtils, HashMap<Integer, GLConsumerThread> glConsumerThreadMap){
        this.jdbcUtils=jdbcUtils;
        this.glConsumerThreadMap=glConsumerThreadMap;
    }
    //缓存的行
    private ArrayBlockingQueue<String> lineStringQueue=new ArrayBlockingQueue<>(Config.maxGLTailCacheSize);



    public void run() {
        String path=null;
        try{
            path=jdbcUtils.getGeneralLogPath();
        }catch (SQLException e){
            logger.error("get general log path failed",e);
            return;
        }

        try{
//            //初始化connect和initdb线程，用于更新processlist，解决短链接找不到DB的问题
//            GLDBConsumerThread gldbConsumerThread=new GLDBConsumerThread(new ArrayList<>(glConsumerThreadMap.values()));
//            gldbConsumerThread.start();

            //初始化tail listener
            TailerListener listener  = new TailerListener() {
                Tailer tailer=null;
                @Override
                public void init(Tailer tailer) {
                    this.tailer=tailer;
                }

                @Override
                public void fileNotFound() {

                }

                @Override
                public void fileRotated() {

                }

                @Override
                public void handle(String line) {
                    try{
                        lineStringQueue.put(line);

                    }catch (Exception e){

                    }
                }

                @Override
                public void handle(Exception ex) {

                }
            };
            File file = new File(path);
            Tailer tailer = new Tailer(file, listener,1000,true);
            Thread thread = new Thread(tailer);
            thread.setDaemon(true); // optional
            thread.start();


            boolean fetchStart=false;
            String line=null;
            StringBuilder buf=new StringBuilder();
            while(isRunning){
                line =lineStringQueue.take();

                if(line!=null) {
                    //如果是connect init db事件，那么需要处理，fetchStart置为false
                    boolean x=applyGLDBEvent(line);
                    if(x){
                        fetchStart=false;
                        continue;
                    }

                    if (fetchStart && MysqlGLUtil.matchEnd(line)) {
                        Integer key = (int) (eventCount % Config.getSqlThreadCnt());
                        glConsumerThreadMap.get(key).add(buf);
                        fetchStart = false;
                        eventCount++;
                    }
                    if (MysqlGLUtil.matchStart(line)) {
                        if (!fetchStart) {
                            fetchStart = true;
                            buf = new StringBuilder();
                        }
                    }
                    if (fetchStart) {
                        buf.append(line);
                        buf.append("\n");
                    }
                }

            }
        }catch (Exception e){
            logger.error("open general log failed",e);
            return;
        }
    }

    //处理connect和init db 事件
    private boolean applyGLDBEvent(String  glEvent){
        try{
            if(MysqlGLUtil.matchConnect(glEvent)){
                String[] strArr=glEvent.split("\\s+");
                ProcessModel model=new ProcessModel();
                if(strArr.length<7 || strArr.length>8){
                    return true;
                }
                model.setId(Integer.valueOf(strArr[1]));
                String userHost=strArr[3];
                String[] userHostArr=userHost.split("@");
                if(userHostArr.length==2){
                    model.setHost(userHostArr[1]);
                    model.setUser(userHostArr[0]);
                }else{
                    model.setHost(strArr[3]);
                    model.setUser("");
                }
                if(strArr.length==8){
                    model.setDB(strArr[5]);
                }else if(strArr.length==7){
                    model.setDB("");
                }
                meta.updateGLModel(model);
                return true;
            }else if(MysqlGLUtil.matchInitDB(glEvent)){

                String[] strArr=glEvent.split("\\s+");
                ProcessModel model=new ProcessModel();
                if(strArr.length!=5){
                    return true;
                }
                model.setId(Integer.valueOf(strArr[1]));
                model.setDB(strArr[4]);
                //为了安全，设计上需要等待当前消费线程消费完存量消息，避免因为processlist的更新导致未消费完的sql匹配到错误的库名
                boolean wait=true;
                while (wait){
                    wait=false;
                    for (GLConsumerThread thread:glConsumerThreadMap.values()){
                        if(thread.getQueueSize()>0){
                            wait=true;
                            try{
                                Thread.sleep(1);
                            }catch (InterruptedException e){

                            }
                            break;
                        }
                    }
                }
                meta.updateGLModel(model);
                return true;
            }
        }catch (Exception e){
            logger.error("apply connect、init db failed",e);
        }
        return false;
    }


//    public void run() {
//        String path=null;
//        try{
//            path=jdbcUtils.getGeneralLogPath();
//        }catch (SQLException e){
//            logger.error("get general log path failed",e);
//            return;
//        }
//        RandomAccessFile file=null;
//        try{
//            file=new RandomAccessFile(path,"r");
//            file.seek(file.length()-1);
//            StringBuilder buf=new StringBuilder();
//            boolean fetchStart=false;
//            String line=null;
//            long spinCount=0l;
//            while(isRunning){
//                if(spinCount<10 && (file.length()-file.getFilePointer())<500){
//                    Thread.sleep(10);
//                    spinCount++;
//                    continue;
//                }
//                spinCount=0l;
//                line =file.readLine();
//                if(line!=null){
////                    if(line.length()<10){
////                        System.out.println(line);
////                    }
//
//                    if(fetchStart && MysqlGLUtil.matchEnd(line)){
//                        Integer key=(int)(eventCount % Config.getSqlThreadCnt());
//                        glConsumerThreadMap.get(key).add(buf);
//                        fetchStart=false;
//                        eventCount++;
//                    }
//                    if(MysqlGLUtil.matchStart(line)){
//                        if(!fetchStart){
//                            fetchStart=true;
//                            buf=new StringBuilder();
//                        }
//                    }
//                    if(fetchStart){
//                        buf.append(line);
//                        buf.append("\n");
//                    }
//                }
//            }
//        }catch (Exception e){
//            logger.error("open general log failed",e);
//            return;
//        }
//    }

    public void close(){
        isRunning=false;
    }

    //等待所有消费线程消费完缓存所有内容
    private void waitGLConsumerClean(){
        boolean wait=true;
        while(wait){
            wait=false;
            for(GLConsumerThread thread:glConsumerThreadMap.values()){
                if(thread.getQueueSize()>0){
                    wait=true;
                    break;
                }
            }
            try{
                Thread.sleep(1);
            }catch (InterruptedException e){

            }
        }
    }

}
