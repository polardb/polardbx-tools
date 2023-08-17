package com.aliyun.gts.sniffer.common.utils;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONArray;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.core.Config;
import com.mysql.cj.jdbc.StatementImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PolarOWrapper  extends JDBCWrapper{
    private Logger logger= LoggerFactory.getLogger(PolarOWrapper.class);

    public PolarOWrapper(String url, String username, String pwd, String database)throws SQLException {
        super(url,username,pwd,database);
    }

    public void initJDBC(String url,String username,String pwd,String database,String currentSchema)throws SQLException{
        this.database=database;
        if(currentSchema==null){
            this.currentSchema=username+",public";
        }else{
            this.currentSchema=currentSchema;
        }
        this.url=url;
        this.username=username;
        this.pwd=pwd;
        try{
            Class.forName("com.aliyun.polardb.Driver");
            conn = DriverManager.getConnection(url+"?currentSchema="+this.currentSchema, username , pwd );
//            this.charset=((JdbcConnection)conn).getSession().getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
            //默认打开polar_comp_stmt_level_tx，避免因为报错导致事务异常。
            if(!Config.disableTransaction){
                conn.setAutoCommit(false);
                try(Statement stmt=conn.createStatement()){
                    stmt.execute("set polar_comp_stmt_level_tx=on");
                    conn.commit();
                }
            }

        }catch(SQLException e){
            throw e;
        }catch (ClassNotFoundException e){
            throw  new RuntimeException(e);
        }
    }

    //用于重放sql，流式读取，防止内存打爆
    @Override
    public Long replay(String sql, int timeout, BaseSQLType sqlType, JSONArray parameter) throws SQLException{
        //检查空闲是否过长或者链接关闭
        if(checkExpired() || conn.isClosed()){
            reconnect();
        }
        updateTS=System.currentTimeMillis();
        Statement stmt=null;
        ResultSet resultSet=null;
        try{
            if(Config.enableStreamRead){
                stmt=conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.FETCH_FORWARD);
                stmt.setFetchSize(100);
            }else{
                stmt=conn.createStatement();
            }
            stmt.setQueryTimeout(timeout);
            //对于DML和DQL，使用explain analyze 获取内核执行时间，排除网络和JDBC处理的影响。
            if(Config.excludeNetworkRound &&(sqlType==BaseSQLType.DML || sqlType==BaseSQLType.DQL)){
                long start=System.nanoTime();
                resultSet=stmt.executeQuery("explain analyze "+sql);
                long stop=System.nanoTime();
                long rt=(stop-start)/1000;
                //如果关闭，那么返回网络+执行时间
                if(!Config.excludeNetworkRound){
                    return rt;
                }
                String planTimeStr=null;
                String execTimeStr=null;
                while(resultSet.next()){
                    if(execTimeStr==null){
                        execTimeStr=resultSet.getString(1);
                    }else{
                        planTimeStr=execTimeStr;
                        execTimeStr=resultSet.getString(1);
                    }
                }
                if(planTimeStr==null || execTimeStr==null){
                    throw new RuntimeException("can not get explain analyze time");
                }
                Long planTime=getPolarExplainTimeUs(planTimeStr);
                Long execTime=getPolarExplainTimeUs(execTimeStr);
                return planTime+execTime;
            }else{
                long start=System.nanoTime();
                stmt.execute(sql);
                long stop=System.nanoTime();
                long rt=(stop-start)/1000;
                //如果关闭，那么
                if(Config.excludeNetworkRound){
                    rt=rt-Config.dstNetworkRoundMicrosecond;
                }
                //返回微秒
                return rt;
            }
        }catch(SQLException e){
            //参数错误，
//            if(e.getSQLState().equals("08P01")){
//                throw e;
//            //abort transaction,直接跳出rollback掉。
//            }else if(e.getSQLState().equals("25P02")){
//                throw e;
//            }
            //连接断开
            if(e.getSQLState().equals("57P01")||e.getSQLState().equals("57P02")||e.getSQLState().equals("57P03")){
                keepAlive();
                throw e;
            }
            //连接不上
            if(e.getSQLState().equals("08000")||e.getSQLState().equals("08001")||e.getSQLState().equals("08003")
                    ||e.getSQLState().equals("08004")||e.getSQLState().equals("08006")){
                keepAlive();
                throw e;
            }
            throw e;
        }catch (RuntimeException e){
            throw e;
        }
        finally {
            if(resultSet!=null){
                resultSet.close();
            }
            if(stmt!=null){
                stmt.close();
            }
        }
    }

    @Override
    public Long getNetworkRoundMicrosecond()throws SQLException{
        int total=11;
        Double sleepSecond=0.5;
        String sql="select pg_sleep("+sleepSecond+");";
        Long min=0l;
        for (int i=0;i<total;i++){
            try(Statement stmt=conn.createStatement();){
                Long begin=System.nanoTime();
                stmt.execute(sql);
                Long end=System.nanoTime();
                Long diff=(end-begin)/1000-Double.valueOf(sleepSecond*1000000).longValue();
                if(i==0){
                    min=diff;
                }
                if(min>diff){
                    min=diff;
                }
            }catch(SQLException e){
                logger.error("detect network round failed",e);
                throw e;
            }
        }
        //返回网络耗时的平均值
        return min;
    }

    public void beforeExecute(String db)throws SQLException{
        if(StringUtils.isEmpty(db)||db.equals("null")){
            return;
        }
        String searchStr=db+", "+username+", public";
        if(searchStr.equals(currentSchema) && !Config.forceSetSchema){
            return;
        }
        String sql="set search_path="+searchStr;
        try(Statement stmt=conn.createStatement();){
            stmt.execute(sql);
            this.currentSchema=searchStr;
        }catch(SQLException e){
//            logger.error("set search_path失败",e);
            keepAlive();
            throw e;
        }
    }

    /**
     * @param timeStr for example: Planning Time: 0.068 ms
     * @return Long time value, microsecond
     */
    private Long getPolarExplainTimeUs(String timeStr) throws RuntimeException{
        String[] strArr=timeStr.trim().split(" ");
        if(strArr.length<3){
            throw new RuntimeException("not valid polardb explain time value:"+timeStr);
        }
        Double doubleStr=Double.valueOf(strArr[2]);
        Double doubleUs=doubleStr*1000;

        return Long.valueOf(doubleUs.longValue());
    }

}
