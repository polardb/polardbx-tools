package com.aliyun.gts.sniffer.thread.generallog;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.aliyun.gts.sniffer.thread.ConsumerThread;
import com.aliyun.gts.sniffer.thread.SqlConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class GLConsumerThread extends ConsumerThread {
    private Logger logger= LoggerFactory.getLogger(SqlConsumerThread.class);
    //缓存的packet数量
    private ArrayBlockingQueue<StringBuilder> glStringQueue=new ArrayBlockingQueue<>(Config.maxPacketQueueSize);

    public void setSrcJdbcUtils(JDBCUtils jdbcUtils){
        this.srcJdbcUtils=jdbcUtils;
    }

    public void add(StringBuilder glSql) throws InterruptedException{
        glStringQueue.put(glSql);
    }
    public int getQueueSize(){
        return glStringQueue.size();
    }

    //processlist维护列表
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();



    public GLConsumerThread() throws SQLException {
        if(Config.replayTo.equals("mysql")){
            String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql?allowPublicKeyRetrieval=true";
            jdbcUtils=new JDBCUtils(url,Config.getDstUsername(), Config.getDstPassword(),"mysql");
        }
        mysqlProcessListMeta=MysqlProcessListMeta.getInstance();
    }



    @Override
    public void run(){
        while(isRunning){
            try{
                String glSql=glStringQueue.take().toString();
                applyGLSql(glSql.trim());
            }catch (InterruptedException e){
                isRunning=false;
                break;
            }
        }
    }
    private void applyGLSql(String  glSql){
        String[] strArr=glSql.split("\\s+",4);
        if(strArr.length<4){
            return;
        }
        String id=strArr[1];
        String sql=strArr[3];
        ProcessModel processModel=meta.getGLProcessModelByHost(id);
        if(processModel==null){
            return;
        }
        //过滤掉commit rollback begin start transaction
        String sql2=sql.toLowerCase(Locale.ROOT);
        if(sql2.startsWith("begin")||sql2.startsWith("commit")
                ||sql2.startsWith("rollback")||sql2.startsWith("start")){
            return;
        }

        if(filterDefault(sql2)){
            return;
        }
        //是否命中需要输出的sql，如果没命中，那么忽略sql
        if(!filterSQL(sql2)){
            return;
        }

        if(Config.replayTo.equals("stdout")){
            logger.info(sql);
            return;
        }
        if(Config.replayTo.equals("file")){
            try{
                fileResultLogger.info(getSqlJSON(sql,processModel).toJSONString());
                return;
            }catch (Exception e){
                logger.error("get sqlid failed",e);
            }
        }
        if(Config.replayTo.equals("mysql")){

//            MySqlStatementParser parser = new MySqlStatementParser(sql);
//            List<SQLSelectStatement> statementList=parser.parseStatementList();
//            for (SQLSelectStatement sqlStatement:statementList){
//                if(sqlStatement instanceof SQLSelectStatement){
//                    //如果DB为null,说明找不到该连接,忽略该sql
                if(processModel==null){
                    return;
                    //如果连接的DB值不为null,那么需要切换连接到当前的DB,use db
                }
                applyMysql(sql,processModel.getDB(),toSqlId(sql));
//                }
//            }
        }
    }
    private String getSelectStr(SQLSelectStatement sqlSelectStatement){
        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        sqlSelectStatement.accept(visitor);
        return out.toString();
    }

}
