package com.aliyun.gts.sniffer.thread;

import com.alibaba.fastjson.JSONArray;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.common.utils.JDBCWrapper;
import com.aliyun.gts.sniffer.common.utils.MysqlGLUtil;
import com.aliyun.gts.sniffer.core.Config;
import org.apache.log4j.FileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class ConsumerThread extends Thread{
    protected boolean skipSet=true;
    protected boolean skipShow=true;
    protected boolean skipReset=true;
    protected long skipRequestCnt= 0L;
    protected long delay= 0L;
    protected boolean ready=false;

    public boolean isReady() {
        return ready;
    }

    public long getDelay() {
        return delay;
    }

    public long getSkipRequestCnt() {
        return skipRequestCnt;
    }

    private final Logger logger= LoggerFactory.getLogger(ConsumerThread.class);
    protected JDBCWrapper jdbcWrapper=null;
    protected volatile boolean running=true;
    protected volatile boolean closed=false;

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

    private final org.apache.log4j.Logger sqlErrorLogger= org.apache.log4j.Logger.getLogger("errorLog");
    public ConsumerThread(){
        FileAppender fileAppender = (FileAppender) org.apache.log4j.Logger.getLogger("errorLog").getAppender("errorLog");
        fileAppender.setFile("run/"+Config.task+"/error.log");
        fileAppender.activateOptions();

    }
    public HashMap<String, String> getSqlList() {
        return sqlList;
    }

    private final HashMap<String,String> sqlList=new HashMap<>();

    private final HashMap<String,Long> sqlReplayRTMax=new HashMap<String, Long>();
    private final HashMap<String,Long> sqlReplayRTMin=new HashMap<String, Long>();
    private final HashMap<String,Long> sqlReplayRTNum=new HashMap<String, Long>();
    private final HashMap<String,Long> sqlReplayRTSum=new HashMap<String, Long>();
    private final HashMap<String,Long> sqlReplayErrorSum=new HashMap<String, Long>();
    private final HashMap<String,Set<String>> sqlReplaySchema=new HashMap<>();

    private final HashMap<String,Long> originMaxExecTime=new HashMap<>();
    private final HashMap<String,Long> originSumExecTime=new HashMap<>();
    private final HashMap<String,Long> originMinExecTime=new HashMap<>();
    private final HashMap<String,Long> originExecCount=new HashMap<>();

    private final HashMap<String, Set<String>> sqlReplayErrorMsg=new HashMap<>();

    public HashMap<String, Set<String>> getSqlReplayErrorMsg() {
        return sqlReplayErrorMsg;
    }

    public HashMap<String, Long> getOriginMaxExecTime() {
        return originMaxExecTime;
    }

    public HashMap<String, Long> getOriginSumExecTime() {
        return originSumExecTime;
    }

    public HashMap<String, Long> getOriginMinExecTime() {
        return originMinExecTime;
    }

    public HashMap<String, Long> getOriginExecCount() {
        return originExecCount;
    }

    protected long requests= 0L;
    protected long errors= 0L;
    //微秒总耗时
    protected long rtSum= 0L;

    public long getRequests(){
        return requests;
    }

    public long getErrors(){
        return errors;
    }

    public long getRtSum(){
        return rtSum;
    }

    public HashMap<String, Long> getSqlReplayRTMax() {
        return sqlReplayRTMax;
    }

    public HashMap<String, Long> getSqlReplayRTMin() {
        return sqlReplayRTMin;
    }

    public HashMap<String, Long> getSqlReplayRTNum() {
        return sqlReplayRTNum;
    }

    public HashMap<String, Long> getSqlReplayRTSum() {
        return sqlReplayRTSum;
    }

    public HashMap<String, Long> getSqlReplayErrorSum() {
        return sqlReplayErrorSum;
    }


    public void close(){
        running=false;
    }

    private void applyOriginExecTime(String sqlId,Long execTime){
        Long tmpMaxExecTime=originMaxExecTime.get(sqlId);
        Long tmpSumExecTime=originSumExecTime.get(sqlId);
        Long tmpMinExecTime=originMinExecTime.get(sqlId);
        Long tmpExecCount=originExecCount.get(sqlId);
        if(tmpMaxExecTime==null){
            tmpMaxExecTime=execTime;
        }else{
            if(tmpMaxExecTime<execTime){
                tmpMaxExecTime=execTime;
            }
        }

        if(tmpSumExecTime==null){
            tmpSumExecTime=execTime;
        }else{
            tmpSumExecTime+=execTime;
        }

        if(tmpExecCount==null){
            tmpExecCount= 1L;
        }else{
            tmpExecCount++;
        }

        if(tmpMinExecTime==null){
            tmpMinExecTime=execTime;
        }else{
            if(tmpMinExecTime>execTime){
                tmpMinExecTime=execTime;
            }
        }
        originMaxExecTime.put(sqlId,tmpMaxExecTime);
        originMinExecTime.put(sqlId,tmpMinExecTime);
        originSumExecTime.put(sqlId,tmpSumExecTime);
        originExecCount.put(sqlId,tmpExecCount);

    }

    public HashMap<String, Set<String>> getSqlReplaySchema() {
        return sqlReplaySchema;
    }

    /**
     * *
     * @param sql 原始sql
     * @param sql2 小写的 去除头部hint的sql，方便进行sql类型判断
     * @param db
     * @param sqlId
     * @param originExecTime
     */
    protected void apply(String sql, String sql2, String db, String sqlId, long originExecTime, BaseSQLType sqlType, JSONArray parameter){
        if(!sqlList.containsKey(sqlId)){
            sqlList.put(sqlId,sql);
        }
        if(!Config.disableInsert2Replace && (Config.replayTo.equals("polarx")||Config.replayTo.equals("mysql"))) {
            if(sql2.toLowerCase(Locale.ROOT).startsWith("insert")
                    &&!MysqlGLUtil.matchReplace(sql2)){
                sql=sql.replaceFirst("(?i)insert","replace");
            }
        }

        if(!sqlReplaySchema.containsKey(sqlId)){
            Set<String> schemaSet=new HashSet<>();
            schemaSet.add(db);
            sqlReplaySchema.put(sqlId,schemaSet);
        }else{
            sqlReplaySchema.get(sqlId).add(db);
        }
        //统计原始sql在源库的执行时间
        applyOriginExecTime(sqlId,originExecTime);
        for (int i = 0; i< Config.enlarge; i++){
            try{
                requests++;
                //如果sql已经报过错了，且开启了跳过sql报错开关，不重复回放已报错sql
                if(sqlReplayErrorSum.get(sqlId)!=null && Config.skipDupliErrorSql){
                    long x=sqlReplayErrorSum.get(sqlId);
                    x++;
                    errors++;
                    sqlReplayErrorSum.put(sqlId,x);
                    continue;
                }
                //mysql:use db
                //polardb:set search_path=public,schema
                if(Config.searchPath!=null){
                    jdbcWrapper.beforeExecute(Config.searchPath);
                }else{
                    jdbcWrapper.beforeExecute(db);
                }
//                System.out.printf("===%s===\n", sql);
                Long rt=jdbcWrapper.replay(sql,Config.sqlTimeout,sqlType,parameter);
//                System.out.printf("===%s===\n", rt);
                //如果超过了Config.excludeLongQueryTime，那么不统计
                if(rt>Config.excludeLongQueryTime && Config.excludeLongQueryTime>0){
                    return;
                }
                applyRT(sqlId,rt);
            }catch (Exception e){
                applyErrorSum(sqlId,e);
                if(parameter==null){
                    sqlErrorLogger.error("sql apply failed,sqlId:"+sqlId+",schema:"+db+",sql:"+sql+"\n paramter:",e);
                }else{
                    sqlErrorLogger.error("sql apply failed,sqlId:"+sqlId+",schema:"+db+",sql:"+sql+"\n paramter:"+parameter.toJSONString(),e);
                }
                errors++;

            }finally {
                try{
                    if(!Config.disableTransaction){
                        if(Config.commit){
                            jdbcWrapper.commit();
                        }else{
                            jdbcWrapper.rollback();
                        }
                    }
                }catch (SQLException ignored){

                }
            }
        }
    }

    protected void applyRT(String sqlId,long rt){
        applySum(sqlId,rt);
        applyNum(sqlId,rt);
        applyMax(sqlId,rt);
        applyMin(sqlId,rt);
        rtSum+=rt;
    }
    private void applyNum(String sqlId,long rt){
        Long x=sqlReplayRTNum.get(sqlId);
        if(x==null){
            x= 1L;
        }else{
            x++;
        }
        sqlReplayRTNum.put(sqlId,x);

    }
    private void applySum(String sqlId,long rt){
        Long x=sqlReplayRTSum.get(sqlId);
        if(x==null){
            x=rt;
        }else{
            x+=rt;
        }
        sqlReplayRTSum.put(sqlId,x);
    }

    private void applyErrorSum(String sqlId,Exception e){
        Long x=sqlReplayErrorSum.get(sqlId);
        if(x==null){
            x= 1L;
        }else{
            x++;
        }
        sqlReplayErrorSum.put(sqlId,x);
        Set<String> errorSet=this.sqlReplayErrorMsg.get(sqlId);
        if(errorSet==null){
            errorSet= new HashSet<>();
            errorSet.add(e.getMessage());
            sqlReplayErrorMsg.put(sqlId,errorSet);
        }else{
            //单条模板sql 错误信息最多5条
            if(errorSet.size()>=Config.maxErrorMsgSize){
                logger.debug(sqlId+" error msg count >5,skip!!!");
                return;
            }
            errorSet.add(e.getMessage());
        }
    }

    private void applyMin(String sqlId,long rt){
        Long x=sqlReplayRTMin.get(sqlId);
        if(x==null){
            x=rt;
        }else{
            if(x>rt){
                x=rt;
            }
        }
        sqlReplayRTMin.put(sqlId,x);
    }

    private void applyMax(String sqlId,long rt){
        Long x=sqlReplayRTMax.get(sqlId);
        if(x==null){
            x=rt;
        }else{
            if(x<rt){
                x=rt;
            }
        }
        sqlReplayRTMax.put(sqlId,x);
    }

    protected boolean filterDefault(String sql){
        //跳过set
        if(skipSet){
            if(sql.startsWith("set")){
                return true;
            }
        }
        //跳过show
        if(skipShow){
            if(sql.startsWith("show")){
                return true;
            }
        }
        //跳过start
        if(skipReset){
            if(sql.startsWith("reset")){
                return true;
            }
        }

        return false;
    }

    protected BaseSQLType getSQLType(String sql){
        //filter DQL
        if(sql.startsWith("select")){
            return BaseSQLType.DQL;
        }
        if(sql.startsWith("update")){
            return BaseSQLType.DML;
        }
        if(sql.startsWith("delete")){
            return BaseSQLType.DML;
        }
        if(sql.startsWith("insert")){
            return BaseSQLType.DML;
        }
        if(sql.startsWith("replace")){
            return BaseSQLType.DML;
        }
        if(sql.startsWith("merge")){
            return BaseSQLType.DML;
        }
        return BaseSQLType.OTHER;
    }



    protected boolean filterSQL(String sql,BaseSQLType sqlType){
        if(Config.filter.contains("ALL")){
            return true;
        }

        //filter DQL
        if(Config.filter.contains("DQL")){
            if(sqlType==BaseSQLType.DQL){
                return true;
            }
        }
        //filter DQL
        if(Config.filter.contains("DML")){
            return sqlType == BaseSQLType.DML;
        }
        return false;
    }

}
