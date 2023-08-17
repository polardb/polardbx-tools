package com.aliyun.gts.slssniffer;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SampleLogHubProcessor implements ILogHubProcessor {
    private int shardId;
    // 记录上次持久化Checkpoint的时间。
    private long mLastCheckTime = 0;

    public void initialize(int shardId) {
        this.shardId = shardId;
    }
    public static long total=0l;
    public static BufferedWriter bufferedWriter =null;
    public static Object objLock=new Object();
    static {
            if(bufferedWriter==null){
                try{
                    File file=new File(SlsSniffer.outputFile);
                    if(!file.exists()){
                        file.createNewFile();
                    }
                    bufferedWriter=new BufferedWriter(new FileWriter(new File(SlsSniffer.outputFile),false));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

    }

    // 消费数据的主逻辑，消费时的所有异常都需要处理，不能直接抛出。
    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker) {
        // 打印已获取的数据。
        for (LogGroupData logGroup : logGroups) {
            FastLogGroup flg = logGroup.GetFastLogGroup();
//            System.out.println(String.format("\tcategory\t:\t%s\n\tsource\t:\t%s\n\ttopic\t:\t%s\n\tmachineUUID\t:\t%s",
//                    flg.getCategory(), flg.getSource(), flg.getTopic(), flg.getMachineUUID()));
//            System.out.println("Tags");
            long curTimestamp=0l;
//            for (int tagIdx = 0; tagIdx < flg.getLogTagsCount(); ++tagIdx) {
//                FastLogTag logtag = flg.getLogTags(tagIdx);
//                if(logtag.getKey().equals("__receive_time__")){
//                    curTimestamp=Long.valueOf(logtag.getValue());
//                }
//                if(curTimestamp>SlsSniffer.endTime){
//                    SlsSniffer.threadCurrentTS.put(Thread.currentThread().getName(), curTimestamp);
//                    return null;
//                }
//            }
            for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                FastLog log = flg.getLogs(lIdx);
//                System.out.println("--------\nLog: " + lIdx + ", time: " + log.getTime() + ", GetContentCount: " + log.getContentsCount());
                curTimestamp=Long.valueOf(log.getTime());
//                if(curTimestamp>SlsSniffer.endTime){
////                    SlsSniffer.threadCurrentTS.put(Thread.currentThread().getName(), curTimestamp);
////                    System.out.println(Thread.currentThread().getName()+":endTime:"+curTimestamp+"###curTime:"+curTimestamp);
//                    return null;
//                }
                if(SlsSniffer.curTimestamp<curTimestamp*1000){
                    SlsSniffer.curTimestamp=curTimestamp*1000;
                }
                String outStr=null;
                if(SlsSniffer.logType.equals("drds")){
                    outStr=getDrdsLog(log);
                }else if(SlsSniffer.logType.equals("rds-mysql")){
                    outStr=getRdsMysqlLog(log);
                }else{
                    outStr=getDefault(log);
                }
                if(outStr==null){
                    continue;
                }
                try{
                    synchronized (objLock){
                        bufferedWriter.write(outStr);
                        bufferedWriter.write("\n");
                        total++;
                    }
                    //每1W行或者每5秒flush一次
                    if(total%10000==0 || (System.currentTimeMillis()/1000)%5==0){
                        bufferedWriter.flush();
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
        long curTime = System.currentTimeMillis();
        // 每隔30秒，写一次Checkpoint到服务端。如果30秒内发生Worker异常终止，新启动的Worker会从上一个Checkpoint获取消费数据，可能存在少量的重复数据。
        if (curTime - mLastCheckTime > 30 * 1000) {
            try {
                //参数为true表示立即将Checkpoint更新到服务端；false表示将Checkpoint缓存在本地，默认间隔60秒会将Checkpoint更新到服务端。
                checkPointTracker.saveCheckPoint(true);
            } catch (LogHubCheckPointException e) {
                e.printStackTrace();
            }
            mLastCheckTime = curTime;
        }
        return null;
    }

    // 当Worker退出时，会调用该函数，您可以在此处执行清理工作。
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        //将Checkpoint立即保存到服务端。
        try {
            bufferedWriter.flush();
            checkPointTracker.saveCheckPoint(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String getDrdsLog(FastLog log){
        String sortValue=null;
        JSONObject object=new JSONObject();
        String ip="";
        String port="";
        //过滤掉不符合filterMap规则的日志
        boolean skip=false;
        if(SlsSniffer.filterMap!=null){
            skip=true;
            for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                FastLogContent content = log.getContents(cIdx);
                if(SlsSniffer.filterMap.containsKey(content.getKey())){
                    skip=false;
                    if(!SlsSniffer.filterMap.get(content.getKey()).equals(content.getValue())){
                        return null;
                    }
                }
            }
        }
        //如果没有命中过滤列，那么直接丢弃
        if(skip){
            return null;
        }
        for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
            FastLogContent content = log.getContents(cIdx);
            if(SlsSniffer.sortByDate){
                if(content.getKey().equals("sql_time")){
                    sortValue=content.getValue();
                }
            }
            try{
                if(content.getKey().equals("sql")){
                    //忽略truncate的对象
                    if(content.getValue().endsWith(" more")){
                        object=null;
                        break;
                    }
                    object.put("convertSqlText",content.getValue());
                }
                if(content.getKey().equals("sql_time")){
                    object.put("startTime", DateUtil.toDate(content.getValue()).getTime()*1000);
                }
                if(content.getKey().equals("response_time")){
                    object.put("execTime",Long.valueOf(content.getValue())*1000);
                }
                if(content.getKey().equals("parameters")){
                    object.put("parameter",content.getValue());
                }
                if(content.getKey().equals("user")){
                    object.put("user",content.getValue());
                }
                if(content.getKey().equals("db_name")){
                    object.put("schema",content.getValue());
                }
                if(content.getKey().equals("client_ip")){
                    ip=content.getValue();
                }
                if(content.getKey().equals("client_port")){
                    port=content.getValue();
                }
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }
        if(object==null){
            return null;
        }
        object.put("session",ip+":"+port);
        if(sortValue==null){
            return object.toJSONString();
        }else{
            return sortValue+"-###@@@###-"+object.toJSONString();

        }
    }

    public String getRdsMysqlLog(FastLog log){
        String sortValue=null;
        JSONObject object=new JSONObject();
        //过滤掉不符合filterMap规则的日志
        boolean skip=false;
        if(SlsSniffer.filterMap!=null){
            skip=true;
            for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                FastLogContent content = log.getContents(cIdx);
                if(SlsSniffer.filterMap.containsKey(content.getKey())){
                    skip=false;
                    if(!SlsSniffer.filterMap.get(content.getKey()).equals(content.getValue())){
                        return null;
                    }
                }
            }
        }
        //如果没有命中过滤列，那么直接丢弃
        if(skip){
            return null;
        }
        for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
            FastLogContent content = log.getContents(cIdx);
            if(SlsSniffer.sortByDate){
                if(content.getKey().equals("origin_time")){
                    Long ts=Long.valueOf(content.getValue())/1000;
                    sortValue=DateUtil.toChar(ts);
                }
            }
            try{
                if(content.getKey().equals("sql")){
                    //简单忽略不带参数的prepare语句
                    if(content.getValue().contains("?") || content.getValue().startsWith("log")){
                        object=null;
                        break;
                    }
                    object.put("convertSqlText",content.getValue());
                }
                if(content.getKey().equals("origin_time")){
                    object.put("startTime", content.getValue());
                }
                if(content.getKey().equals("latency")){
                    object.put("execTime",Long.valueOf(content.getValue()));
                }
                if(content.getKey().equals("user")){
                    object.put("user",content.getValue());
                }
                if(content.getKey().equals("db")){
                    object.put("schema",content.getValue());
                }
                if(content.getKey().equals("thread_id")){
                    object.put("session",content.getValue());
                }
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }
        if(object==null){
            return null;
        }
        try{
            if(sortValue==null){
                return object.toJSONString();
            }else{
                return sortValue+"-###@@@###-"+object.toJSONString();
            }
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }
    public String getDefault(FastLog log){
        JSONObject object=new JSONObject();
        //过滤掉不符合filterMap规则的日志
        boolean skip=false;
        if(SlsSniffer.filterMap!=null){
            skip=true;
            for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                FastLogContent content = log.getContents(cIdx);
                if(SlsSniffer.filterMap.containsKey(content.getKey())){
                    skip=false;
                    if(!SlsSniffer.filterMap.get(content.getKey()).equals(content.getValue())){
                        return null;
                    }
                }
            }
        }
        //如果没有命中过滤列，那么直接丢弃
        if(skip){
            return null;
        }
        for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
            FastLogContent content = log.getContents(cIdx);
            try{
//                if(SlsSniffer.filterMap!=null){
//                    if(SlsSniffer.filterMap.containsKey(content.getKey())){
//                        if(!SlsSniffer.filterMap.get(content.getKey()).equals(content.getValue())){
//                            return null;
//                        }
//                    }else{
//                        return null;
//                    }
//                }
                object.put(content.getKey(),content.getValue());
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }
        return object.toJSONString();
    }

}

class SampleLogHubProcessorFactory implements ILogHubProcessorFactory {
    public ILogHubProcessor generatorProcessor() {
        // 生成一个消费实例。
        return new SampleLogHubProcessor();
    }
}
