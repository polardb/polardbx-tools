package com.aliyun.gts.sniffer.common.utils;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONArray;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.core.Config;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class MysqlWrapper extends JDBCWrapper{
    private Logger logger = LoggerFactory.getLogger(MysqlWrapper.class);

    public MysqlWrapper(String url, String username, String pwd, String schemaName)throws SQLException{
        super(url,username,pwd,schemaName);
    }

    @Override
    public void initJDBC(String url,String username,String pwd,String database,String currentSchema)throws SQLException{
        this.database=database;
        this.url=url;
        this.username=username;
        this.pwd=pwd;
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url+"/"+this.database+"?allowPublicKeyRetrieval=true&connectTimeout=60000" , username , pwd );
            if(!Config.disableTransaction){
                conn.setAutoCommit(false);
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
        Statement stmt2=null;
        try{
            if(parameter==null || parameter.size()==0){
                if(Config.enableStreamRead){
                    stmt2=conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    ((StatementImpl)stmt2).enableStreamingResults();
                }else{
                    stmt2=conn.createStatement();
                }
                stmt2.setQueryTimeout(timeout);
                long start=System.nanoTime();
                if(Config.excludeNetworkRound && Config.replayTo.equals("polarx") && (sqlType==BaseSQLType.DML || sqlType==BaseSQLType.DQL)){
                    stmt2.execute("explain analyze "+sql);
                }else{
                    stmt2.execute(sql);
                }

                long stop=System.nanoTime();
                long rt=(stop-start)/1000;
                //如果关闭，那么
                if(Config.excludeNetworkRound){
                    Long tmpRT=rt-Config.dstNetworkRoundMicrosecond;
                    if(tmpRT>0){
                        rt=tmpRT;
                    }
                }
                return rt;
            }else{
                //preparestatement
                if(Config.excludeNetworkRound && Config.replayTo.equals("polarx") && (sqlType==BaseSQLType.DML || sqlType==BaseSQLType.DQL)){
                    stmt2=conn.prepareStatement("explain analyze "+sql);
                }else{
                    stmt2=conn.prepareStatement(sql);
                }

                for(int i=0;i<parameter.size();i++){
                    ((PreparedStatement) stmt2).setObject(i+1,parameter.getString(i));
                }
                stmt2.setQueryTimeout(timeout);
                long start=System.nanoTime();
                ((PreparedStatement) stmt2).execute();
                long stop=System.nanoTime();
                long rt=(stop-start)/1000;
                //如果关闭，那么
                if(Config.excludeNetworkRound){
                    Long tmpRT=rt-Config.dstNetworkRoundMicrosecond;
                    if(tmpRT>0){
                        rt=tmpRT;
                    }
                }
                return rt;
            }


        } catch (CommunicationsException | SQLNonTransientConnectionException e){
            //重连之前关闭，避免出现ResultsetRowsStreaming@5800daf5 is still active 异常
            if(stmt2!=null){
                stmt2.close();
            }
            keepAlive();
            throw e;
        }finally {
            //确保stmt2关闭，避免出现ResultsetRowsStreaming@5800daf5 is still active 异常
            if(stmt2!=null){
                stmt2.close();
            }
        }
    }

    @Override
    public Long getNetworkRoundMicrosecond()throws SQLException{
        int total=11;
        Double sleepSecond=0.5;
        String sql="select sleep("+sleepSecond+");";
        Long min=0l;
        for (int i=0;i<total;i++){
            try(Statement stmt=conn.createStatement();){
                Long begin=System.nanoTime();
                stmt.execute(sql);
                Long end=System.nanoTime();
                Long diff=(end-begin)/1000-Double.valueOf(sleepSecond*1000000).longValue();
                //排除掉第一个，一般第一个耗时较大
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
        //防止重复切换
        if(db.equals(database)){
            return;
        }
        String sql="use "+db;
        Statement stmt2=null;
        try{
            stmt2=conn.createStatement();
            stmt2.execute(sql);
            this.database=db;
        }catch (CommunicationsException | SQLNonTransientConnectionException e){
            if(stmt2!=null){
                stmt2.close();
            }
            keepAlive();
            throw e;
        }finally {
            if(stmt2!=null){
                stmt2.close();
            }
        }
//        catch(SQLException e){
//            //logger.error("use DB失败",e);
//            keepAlive();
//            throw e;
//        }

    }

    public boolean generalLogOpened() throws SQLException{
        String sql="show variables like 'general_log'";
        try(Statement stmt=conn.createStatement();){
            ResultSet resultSet= stmt.executeQuery(sql);
            resultSet.next();
            String value=resultSet.getString("Value");
            return value.equals("ON");
        }
    }

    public String getGeneralLogPath() throws SQLException{
        String sql="show variables like 'general_log_file'";
        try(Statement stmt=conn.createStatement();){
            ResultSet resultSet= stmt.executeQuery(sql);
            resultSet.next();
            String value=resultSet.getString("Value");
            return value;
        }

    }


    public boolean isEscapeNeededForString(String x, int stringLength) {
        boolean needsHexEscape = false;

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                case '\n': /* Must be escaped for logs */
                case '\r':
                case '\\':
                case '\'':
                case '"': /* Better safe than sorry */
                case '\032': /* This gives problems on Win32 */
                    needsHexEscape = true;
                    break;
            }

            if (needsHexEscape) {
                break; // no need to scan more
            }
        }
        return needsHexEscape;
    }


    public String escapeMysqlString(String x) {
        int stringLength = x.length();
        Session session = ((JdbcConnection) conn).getSession();
        String charEncoding = session.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
        CharsetEncoder charsetEncoder = Charset.forName(charEncoding).newEncoder();
        if (session.getServerSession().isNoBackslashEscapesSet()) {
            // Scan for any nasty chars
            boolean needsHexEscape = isEscapeNeededForString(x, stringLength);
            if (!needsHexEscape) {
                StringBuilder quotedString = new StringBuilder(x.length() + 2);
                quotedString.append('\'');
                quotedString.append(x);
                quotedString.append('\'');
                return quotedString.toString();
            } else {
                return x;
            }
        }

        String parameterAsString = x;
        boolean needsQuoted = true;

        if (isEscapeNeededForString(x, stringLength)) {
            needsQuoted = false; // saves an allocation later

            StringBuilder buf = new StringBuilder((int) (x.length() * 1.1));

            buf.append('\'');

            //
            // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
            //

            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);

                switch (c) {
                    case 0: /* Must be escaped for 'mysql' */
                        buf.append('\\');
                        buf.append('0');
                        break;
                    case '\n': /* Must be escaped for logs */
                        buf.append('\\');
                        buf.append('n');
                        break;
                    case '\r':
                        buf.append('\\');
                        buf.append('r');
                        break;
                    case '\\':
                        buf.append('\\');
                        buf.append('\\');
                        break;
                    case '\'':
                        buf.append('\'');
                        buf.append('\'');
                        break;
                    case '"': /* Better safe than sorry */
                        if (session.getServerSession().useAnsiQuotedIdentifiers()) {
                            buf.append('\\');
                        }
                        buf.append('"');
                        break;
                    case '\032': /* This gives problems on Win32 */
                        buf.append('\\');
                        buf.append('Z');
                        break;
                    case '\u00a5':
                    case '\u20a9':
                        // escape characters interpreted as backslash by mysql
                        if (charsetEncoder != null) {
                            CharBuffer cbuf = CharBuffer.allocate(1);
                            ByteBuffer bbuf = ByteBuffer.allocate(1);
                            cbuf.put(c);
                            cbuf.position(0);
                            charsetEncoder.encode(cbuf, bbuf, true);
                            if (bbuf.get(0) == '\\') {
                                buf.append('\\');
                            }
                        }
                        buf.append(c);
                        break;

                    default:
                        buf.append(c);
                }
            }

            buf.append('\'');

            parameterAsString = buf.toString();
        }
        return parameterAsString;

    }


}
