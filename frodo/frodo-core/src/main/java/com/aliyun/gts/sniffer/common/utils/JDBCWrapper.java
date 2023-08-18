package com.aliyun.gts.sniffer.common.utils;

import com.alibaba.fastjson.JSONArray;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.core.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

public abstract class JDBCWrapper {
    private Logger logger = LoggerFactory.getLogger(JDBCWrapper.class);
    protected Connection conn=null;
    protected String database;
    protected String url;
    protected String username;
    protected String pwd;
    //    protected String charset;
    protected String currentSchema;
    protected long updateTS=System.currentTimeMillis();//每次重放sql时，更新该时间戳，用于判断超长链接自动断链问题

    public String getUrl() {
        return url;
    }



    public JDBCWrapper(String url, String username, String pwd, String database)throws SQLException{
        initJDBC(url,username,pwd,database,null);
    }

    abstract void initJDBC(String url,String username,String pwd,String database,String currentSchema)throws SQLException;

    public String getDatabase()throws SQLException{
        return database;
    }

    //关闭连接
    public void close(){
        if(conn==null){
            return;
        }
        try{
            conn.close();
        }catch (SQLException e){
            return;
        }
    }

    public abstract void beforeExecute(String str) throws SQLException;

    protected boolean checkExpired(){
        long msDiff=System.currentTimeMillis()-this.updateTS;
        if(msDiff> Config.maxConnectionExpiredMS){
            return true;
        }
        return false;
    }

    //获取网络来回时间，在计算网络耗时时，排除耗时。
    public abstract Long getNetworkRoundMicrosecond() throws SQLException;

    public void commit() throws SQLException{
        conn.commit();
    }

    public void rollback() throws SQLException{
        conn.rollback();
    }

    public void setAutoCommit(Boolean autoCommit) throws SQLException{
        conn.setAutoCommit(autoCommit);
    }

    /**
     * *
     * @param sql
     * @param timeout
     * @param sqlType:DML DQL OTHER
     * @return SQL execution time,unit microsecond
     * @throws SQLException
     */
    public abstract Long  replay(String sql, int timeout, BaseSQLType sqlType, JSONArray parameter) throws SQLException;



    //检查连接是否存活
    public boolean testAlive() throws SQLException{
        if(conn.isClosed()){
            return false;
        }
        Statement stmt2=null;
        try{
            stmt2=conn.createStatement();
            String sql="/* ping */ select 1;";
            stmt2.executeQuery(sql);
            return true;
        }catch(SQLException e){
            logger.warn("detect jdbc closed",e);
        }finally {
            //确保stmt2关闭，避免出现ResultsetRowsStreaming@5800daf5 is still active 异常
            if(stmt2!=null){
                stmt2.close();
            }
        }
        return false;
    }

    //重连
    public void reconnect() throws SQLException{
        try{
            if (conn != null) {
                conn.close();
            }
        }catch (Exception e){
            logger.warn("jdbc 关闭连接出错:",e);
        }
        initJDBC(this.url,this.username,this.pwd,this.database,currentSchema);
    }

    public void keepAlive()throws SQLException{
        if(!testAlive()){
            reconnect();
        }

    }
}
