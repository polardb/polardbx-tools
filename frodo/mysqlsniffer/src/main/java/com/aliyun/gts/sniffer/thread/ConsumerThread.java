package com.aliyun.gts.sniffer.thread;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.MysqlBinaryValue;
import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.common.utils.HexUtil;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.common.utils.MysqlGLUtil;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ConsumerThread extends Thread{
    protected boolean skipSet=true;
    protected boolean skipShow=true;
    protected boolean insert2Ignore=true;
    protected boolean skipReset=true;
    protected static ConcurrentLinkedHashMap<String, List<MysqlBinaryValue>> batchTypeCache=new ConcurrentLinkedHashMap.Builder<String, List<MysqlBinaryValue>>()
            .maximumWeightedCapacity(65535)
            .weigher(Weighers.singleton())
            .build();
    private Logger logger= LoggerFactory.getLogger(ConsumerThread.class);
    protected JDBCUtils jdbcUtils=null;
    protected JDBCUtils srcJdbcUtils=null;
    protected volatile boolean isRunning=true;
    protected MysqlProcessListMeta mysqlProcessListMeta;
    protected org.apache.log4j.Logger SQLErrorLogger= org.apache.log4j.Logger.getLogger("errorLog");
    protected org.apache.log4j.Logger fileResultLogger= org.apache.log4j.Logger.getLogger("resultLog");

    public HashMap<String, String> getSqlList() {
        return sqlList;
    }

    protected HashMap<String,String> sqlList=new HashMap<>();

    protected HashMap<String,Long> sqlReplayRTMax=new HashMap<String, Long>();
    protected HashMap<String,Long> sqlReplayRTMin=new HashMap<String, Long>();
    protected HashMap<String,Long> sqlReplayRTNum=new HashMap<String, Long>();
    protected HashMap<String,Long> sqlReplayRTSum=new HashMap<String, Long>();
    protected HashMap<String,Long> sqlReplayErrorSum=new HashMap<String, Long>();

    protected long requests=0l;
    protected long errors=0l;
    //微秒总耗时
    protected long rtSum=0l;

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
        isRunning=false;
    }

    public void setSrcJdbcUtils(JDBCUtils jdbcUtils){
        this.srcJdbcUtils=jdbcUtils;
    }

    protected JSONObject getSqlJSON(String sql, ProcessModel processModel){
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("convertSqlText",sql);
        jsonObject.put("execTime",0);
        jsonObject.put("sqlId",toSqlId(sql));
        jsonObject.put("startTime",System.currentTimeMillis()*1000);
        jsonObject.put("session",processModel.getId());
        if(processModel!=null){
            jsonObject.put("schema",processModel.getDB());
            jsonObject.put("user",processModel.getUser());
        }else {
            jsonObject.put("schema","");
            jsonObject.put("user","");
        }
        return jsonObject;
    }

    //返回sql参数化后的唯一标识
    protected String toSqlId(String sql){
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                String dbtype = JdbcConstants.MYSQL;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){
            logger.debug("get sqlId failed",e);
        }
        return HexUtil.to16MD5("0");

    }

    protected void applyMysql(String sql,String db,String sqlId){
        if(insert2Ignore){
            if(sql.toLowerCase(Locale.ROOT).startsWith("insert")
            &&!MysqlGLUtil.matchInsertIgnore(sql)){
                sql=sql.replaceFirst("insert","insert ignore");
            }
        }


        if(!sqlList.containsKey(sqlId)){
            sqlList.put(sqlId,sql);
        }
        try{
            jdbcUtils.switchDB(db);
            for (int i = 0; i< Config.getEnlarge(); i++){
                try{
                    long start=System.nanoTime();
                    requests++;
//                    jdbcUtils.execute("select * from orders");
                    //如果是begin、commit、rollback，那么忽略掉
//                    if(sql.trim().toLowerCase(Locale.ROOT).equals("begin")||sql.trim().toLowerCase(Locale.ROOT).equals("commit")
//                    ||sql.trim().toLowerCase(Locale.ROOT).equals("rollback")){
//                        //do nothing
//                    }else{
                    jdbcUtils.replay(sql,Config.sqlTimeout);
//                    }
                    long stop=System.nanoTime();
                    applyRT(sqlId,start,stop);
                }catch (SQLException e){
                    applyErrorSum(sqlId);
                    SQLErrorLogger.error("sql apply failed,sql:"+sql,e);
                    errors++;
                }finally {
                    try{
                        if(Config.commit){
                            jdbcUtils.commit();
                        }else{
                            jdbcUtils.rollback();
                        }
                    }catch (SQLException e){

                    }
                }
            }
        }catch (Exception e){
            SQLErrorLogger.error("switch db failed,db:"+db+"sql:"+sql,e);
        }
    }

    protected void applyRT(String sqlId,long start,long stop){
        //转换成微秒
        long rt=(stop-start)/1000;
        applySum(sqlId,rt);
        applyNum(sqlId,rt);
        applyMax(sqlId,rt);
        applyMin(sqlId,rt);
        rtSum+=rt;
    }
    private void applyNum(String sqlId,long rt){
        Long x=sqlReplayRTNum.get(sqlId);
        if(x==null){
            x=1l;
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

    private void applyErrorSum(String sqlId){
        Long x=sqlReplayErrorSum.get(sqlId);
        if(x==null){
            x=1l;
        }else{
            x++;
        }
        sqlReplayErrorSum.put(sqlId,x);
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

    protected boolean filterSQL(String sql){
        if(Config.filter.contains("ALL")){
            return true;
        }

        //filter DQL
        if(Config.filter.contains("DQL")){
            if(sql.startsWith("select")){
                return true;
            }
        }
        //filter DQL
        if(Config.filter.contains("DML")){
            if(sql.startsWith("update")){
                return true;
            }
            if(sql.startsWith("delete")){
                return true;
            }
            if(sql.startsWith("insert")){
                return true;
            }
            if(sql.startsWith("replace")){
                return true;
            }
        }
        return false;

    }

    protected boolean filterSelectAndDml(String sql){
        sql=sql.toLowerCase(Locale.ROOT);
        if(sql.startsWith("select")){
            return true;
        }
        if(sql.startsWith("update")){
            return true;
        }
        if(sql.startsWith("delete")){
            return true;
        }
        if(sql.startsWith("insert")){
            return true;
        }
        if(sql.startsWith("replace")){
            return true;
        }

        return false;

    }

}
