package com.aliyun.gts.sniffer.thread.offlinereplay;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.aliyun.gts.sniffer.thread.ConsumerThread;
import com.aliyun.gts.sniffer.thread.SqlConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;

public class JSConsumerThread extends ConsumerThread {
    private Logger logger= LoggerFactory.getLogger(SqlConsumerThread.class);
    //计算json文件中的第一条sql的执行时间和当前时间的差值，用于模拟实际速度重放sql
    private long execTimeDiff=0l;
    private volatile  long actTimeDiff;

    //缓存的packet数量
    private ArrayBlockingQueue<String> jsStringQueue=new ArrayBlockingQueue<>(Config.maxPacketQueueSize);
    public void setSrcJdbcUtils(JDBCUtils jdbcUtils){
        this.srcJdbcUtils=jdbcUtils;
    }
    public void add(String json) throws InterruptedException{
        jsStringQueue.put(json);
    }

    public long getQueueSize(){
        return jsStringQueue.size();
    }


    //processlist维护列表
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();


    public JSConsumerThread() throws SQLException {
        if(Config.replayTo.equals("mysql")){
            String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql?allowPublicKeyRetrieval=true";
            jdbcUtils=new JDBCUtils(url,Config.getDstUsername(), Config.getDstPassword(),"mysql");
        }
        mysqlProcessListMeta=MysqlProcessListMeta.getInstance();
    }



    public void close(){
        isRunning=false;
    }
    @Override
    public void run(){
        while(isRunning){
            try{
                String glSql=jsStringQueue.take();
                applyJSONSql(glSql);
            }catch (InterruptedException e){
                isRunning=false;
                break;
            }
        }
    }
    private void applyJSONSql(String  sqlJSON){
        JSONObject object= JSON.parseObject(sqlJSON);
        if(object==null){
            return;
        }
        String sql=object.getString("convertSqlText");
        String db=object.getString("schema");
        String sqlId=object.getString("sqlId");

        //过滤掉commit rollback begin start transaction
        String sql2=sql.toLowerCase(Locale.ROOT);
        if(sql2.startsWith("begin")||sql2.startsWith("commit")
                ||sql2.startsWith("rollback")||sql2.startsWith("start")){
            return;
        }
        //默认过滤掉set reset show等命令
        if(filterDefault(sql2)){
            return;
        }
        //是否命中需要输出的sql，如果没命中，那么忽略sql
        if(!filterSQL(sql2)){
            return;
        }

        //单位微秒
        long startTime=object.getLong("startTime")/1000;
        if(execTimeDiff==0l){
            execTimeDiff= System.currentTimeMillis()-BigDecimal.valueOf(startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue();
        }
        if(Config.replayTo.equals("mysql")){
            //计算预计执行时间和当前执行时间的差值。
            actTimeDiff=BigDecimal.valueOf(startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()+execTimeDiff-System.currentTimeMillis();
            while (actTimeDiff>0 && !Config.circle){
                try{
                    Thread.sleep(actTimeDiff);
                    actTimeDiff=BigDecimal.valueOf(startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()+execTimeDiff-System.currentTimeMillis();
                }catch (InterruptedException e){

                }
            }
            applyMysql(sql,db,sqlId);
        }
    }
    private String getSelectStr(SQLSelectStatement sqlSelectStatement){
        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        sqlSelectStatement.accept(visitor);
        return out.toString();
    }

}
