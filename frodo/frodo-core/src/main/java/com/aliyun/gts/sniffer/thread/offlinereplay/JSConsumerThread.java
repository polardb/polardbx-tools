package com.aliyun.gts.sniffer.thread.offlinereplay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.common.utils.MysqlWrapper;
import com.aliyun.gts.sniffer.common.utils.PolarOWrapper;
import com.aliyun.gts.sniffer.common.utils.Util;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.thread.ConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JSConsumerThread extends ConsumerThread {
    private Logger logger= LoggerFactory.getLogger(JSConsumerThread.class);
    //计算json文件中的第一条sql的执行时间和当前时间的差值，用于模拟实际速度重放sql
    private long execTimeDiff=0l;
    private volatile long actTimeDiff;

    public void setExecTimeDiff(long execTimeDiff) {
        this.execTimeDiff = execTimeDiff;
    }

    //缓存的packet数量
    private ArrayBlockingQueue<String> jsStringQueue=new ArrayBlockingQueue<>(Config.maxPacketQueueSize);

    public void add(String json) throws InterruptedException{
        jsStringQueue.put(json);
    }

    public long getQueueSize(){
        return jsStringQueue.size();
    }

    public JSConsumerThread() throws SQLException {
        if(Config.replayTo.equals("mysql")){
            String url="jdbc:mysql://"+ Config.host+":"+Config.port;
            jdbcWrapper=new MysqlWrapper(url,Config.username, Config.password,Config.database);
        } else if(Config.replayTo.equals("polarx")){
            String url="jdbc:mysql://"+ Config.host+":"+Config.port;
            jdbcWrapper=new MysqlWrapper(url,Config.username, Config.password,Config.database);
        } else{
            String url="jdbc:polardb://"+ Config.host+":"+Config.port+"/"+Config.database;
            jdbcWrapper=new PolarOWrapper(url,Config.username, Config.password,Config.database);
        }
    }



    public void close(){
        running=false;
    }
    @Override
    public void run(){
        logger.info("consumer thread "+Thread.currentThread().getName()+" start!");
        while(running){
            try{
                String glSql=jsStringQueue.poll(100, TimeUnit.MILLISECONDS);
                if(glSql==null){
                    continue;
                }
                try{
                    applyJSONSql(glSql);
                }catch (Exception e){
                    logger.error("apply sql failed",e);
                }
            }catch (InterruptedException e){
                running=false;
                break;
            }
        }
        delay=0l;
        closed=true;
        logger.info("consumer thread "+Thread.currentThread().getName()+" closed!");
        jdbcWrapper.close();
    }
    private void applyJSONSql(String  sqlJSON){
        JSONObject object= JSON.parseObject(sqlJSON);
        if(object==null){
            return;
        }
        String sql=object.getString("convertSqlText");
        String db=object.getString("schema");
        //默认把schema转成小写。
        if(!Config.diableLowerSchema){
            db=db.toLowerCase(Locale.ROOT);
        }
        String sqlId=object.getString("sqlId");
        long originExecTime=object.getLong("execTime");
        if(Config.sourceDB.equals("oracle")){
            sqlId= Util.toOracleSqlId(sql,sqlId);
        }
        if(Config.sourceDB.equals("polarx")||sqlId==null){
            sqlId= Util.toPolarXSqlId(sql);
        }

        //全小写，方便处理，同时去除select头部的hint，避免hint影响sql语句类型判断
        String sql2=Util.trimHeaderHint(sql.toLowerCase(Locale.ROOT));
        //过滤掉commit rollback begin start transaction
        if(sql2.startsWith("begin")||sql2.startsWith("commit")
                ||sql2.startsWith("rollback")||sql2.startsWith("start")){
            skipRequestCnt++;
            return;
        }

        //默认过滤掉set reset show等命令
        if(filterDefault(sql2)){
            skipRequestCnt++;
            return;
        }
        //判断SQL类型
        BaseSQLType sqlType=getSQLType(sql2);
        //是否命中需要输出的sql，如果没命中，那么忽略sql
        if(!filterSQL(sql2,sqlType)){
            skipRequestCnt++;
            return;
        }
        //单位微秒
        long startTime=object.getLong("startTime")/1000;

        //计算预计执行时间和当前执行时间的差值。
        actTimeDiff=BigDecimal.valueOf(startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()+execTimeDiff-System.currentTimeMillis();
        while (actTimeDiff>0 && !Config.circle){
            try{
                Thread.sleep(actTimeDiff);
                actTimeDiff=BigDecimal.valueOf(startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()+execTimeDiff-System.currentTimeMillis();
            }catch (InterruptedException e){

            }
        }
        delay=actTimeDiff*-1;
        //apply(sql,sql2,db,sqlId,originExecTime,sqlType);

    }
}
