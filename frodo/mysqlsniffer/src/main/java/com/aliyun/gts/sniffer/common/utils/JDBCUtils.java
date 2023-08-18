package com.aliyun.gts.sniffer.common.utils;

import com.aliyun.gts.sniffer.common.entity.MysqlBinaryValue;
import com.aliyun.gts.sniffer.common.entity.MysqlPrepareStmtResponse;
import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mysql.MysqlType;
import com.aliyun.gts.sniffer.thread.SqlConsumerThread;
import com.mysql.cj.ParseInfo;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.util.StringUtils;
import jpcap.packet.TCPPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.*;
import java.util.*;


public class JDBCUtils {
    private Logger logger = LoggerFactory.getLogger(JDBCUtils.class);
    private Connection conn=null;
    private Statement stmt=null ;
    private String schemaName;
    private String url;
    private String username;
    private String pwd;
    private String charset;

    public String getUrl() {
        return url;
    }

    public JDBCUtils(String url, String username, String pwd, String schemaName)throws SQLException{
        initJDBC(url,username,pwd,schemaName);
    }

    private void initJDBC(String url,String username,String pwd,String schemaName)throws SQLException{
        this.schemaName=schemaName;
        this.url=url;
        this.username=username;
        this.pwd=pwd;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn =DriverManager.getConnection(url , username , pwd );
            stmt=conn.createStatement();
            this.charset=((JdbcConnection)conn).getSession().getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
        }catch(SQLException e){
            throw e;
        }catch (ClassNotFoundException e){
            throw  new RuntimeException(e);
        }
    }

    public String getSchema()throws SQLException{
        return schemaName;
    }

    //关闭连接
    public void close(){
        if(stmt!=null){
            try{
                stmt.close();
            }catch (SQLException e){
                //do nothing
            }
        }
        if(conn==null){
            return;
        }
        try{
            conn.close();
        }catch (SQLException e){
            return;
        }
    }

    //用于执行DML语句
    public boolean executeDML(List<String> sqlList) throws SQLException{
        String nowSql="";
        try{
            conn.setAutoCommit(false);
            for (String sql:sqlList){
                nowSql=sql;
                stmt.addBatch(sql);
                logger.info("执行DML:"+sql);
            }
            stmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            return true;
        }catch(SQLException e){
            logger.error("执行DML失败",e);
            conn.rollback();
            conn.setAutoCommit(true);
            throw e;
        }
    }

    public void switchDB(String db)throws SQLException{
        if(com.alibaba.druid.util.StringUtils.isEmpty(db)){
            return;
        }
        //防止重复切换
        if(db.equals(schemaName)){
            return;
        }
        String sql="use "+db;
        try{
            stmt.execute(sql);
            this.schemaName=db;
        }catch(SQLException e){
            logger.error("use DB失败",e);
            keepAlive();
            throw e;
        }

    }

    //用于执行DML语句
    public boolean executeSelect(String sql) throws SQLException{
        try{
            stmt.executeQuery(sql);
            return true;
        }catch(SQLException e){
            keepAlive();
            throw e;
        }
    }

    public void commit() throws SQLException{
        conn.commit();
    }

    public void rollback() throws SQLException{
        conn.rollback();
    }

    public void setAutoCommit(Boolean autoCommit) throws SQLException{
        conn.setAutoCommit(autoCommit);
    }

    //用于执行DML语句
    public boolean execute(String sql) throws SQLException{
        try{
            stmt.execute(sql);
            return true;
        }catch(SQLException e){
            keepAlive();
            throw e;
        }
    }

    //用于重放sql，流式读取，防止内存打爆
    public boolean replay(String sql,int timeout) throws SQLException{
        try(Statement stmt2=conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);){
            conn.setAutoCommit(false);
//            stmt2.setFetchSize(Integer.MIN_VALUE);
            ((StatementImpl)stmt2).enableStreamingResults();
            stmt2.setQueryTimeout(timeout);
            stmt2.execute(sql);
            return true;
        }catch(SQLException e){
            keepAlive();
            throw e;
        }
    }


    public HashMap<String,ProcessModel> getProcessList(){

        HashMap<String,ProcessModel>  processList=new HashMap<String, ProcessModel>();
        try{
            //检查连接,如果断链,那么重连,重连失败,直接抛出异常,返回空的process list元数据,
            testAlive();
            String sql="show processlist";
            ResultSet resultSet= stmt.executeQuery(sql);
            while(resultSet.next()){
                ProcessModel processModel=setProcessModel(resultSet);
                processList.put(processModel.getHost(),processModel);
            }
        }catch(SQLException e){
            logger.error("show processlist 失败",e);
        }
        return processList;
    }

    public HashMap<String,ProcessModel> getGLProcessList(){
        HashMap<String,ProcessModel>  processList=new HashMap<String, ProcessModel>();
        try{
            //检查连接,如果断链,那么重连,重连失败,直接抛出异常,返回空的process list元数据,
            keepAlive();
            String sql="show processlist";
            ResultSet resultSet= stmt.executeQuery(sql);
            while(resultSet.next()){
                ProcessModel processModel=setProcessModel(resultSet);
                processList.put(processModel.getId()+"",processModel);
            }
        }catch(SQLException e){
            logger.error("show processlist 失败",e);
        }
        return processList;
    }

    //从结果集构造一个processlist对象,丢弃其他字段的值,db host user id四个字段已经足够
    private ProcessModel setProcessModel(ResultSet resultSet) throws SQLException{
        ProcessModel processModel=new ProcessModel();
        processModel.setId(resultSet.getLong("ID"));
        processModel.setUser(resultSet.getString("USER"));
        processModel.setHost(resultSet.getString("HOST"));
        processModel.setDB(resultSet.getString("DB"));
        //processModel.setCommand(resultSet.getString("COMMAND"));
        return processModel;

    }

    //检查连接是否存活
    public boolean testAlive(){
        try{
            String sql="/* ping */ select 1";
            ResultSet resultSet= stmt.executeQuery(sql);
            return true;
        }catch(SQLException e){
            logger.warn("检测到jdbc连接失效",e);
            return false;
        }
    }

//    private String res

    public String toSqlStr(String stmtSql, List<MysqlBinaryValue> values) throws SQLException, UnsupportedEncodingException {
//        switchDB(db);
//        PreparedStatement stmt=null;
        StringBuffer buf=new StringBuffer();
        ParseInfo parseInfo=new ParseInfo(stmtSql,((JdbcConnection)conn).getSession(),charset);
        byte[][] statSql=parseInfo.getStaticSql();
//            stmt=conn.prepareStatement(stmtSql);
        int i=1;
        for(MysqlBinaryValue binaryValue:values){
            buf.append(StringUtils.toString(statSql[i-1],charset));
            byte[] data=binaryValue.getData();
            //setString
            if(binaryValue.getType()== MysqlType.MYSQL_TYPE_STRING
                    || binaryValue.getType()== MysqlType.MYSQL_TYPE_VAR_STRING
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_VARCHAR){
                if(Config.stringToHex){
                    buf.append("x'");
                    buf.append(HexUtil.bytesToHexString(data,binaryValue.getBegin(),binaryValue.getLength()));
                    buf.append("'");
                }else{
                    buf.append(escapeMysqlString(new String(data,binaryValue.getBegin(),binaryValue.getLength())));
                }

            }else if(binaryValue.getType()== MysqlType.MYSQL_TYPE_LONG
                    ||binaryValue.getType()== MysqlType.MYSQL_TYPE_INT24){
                buf.append(NumberUtil.fourLEByte2Int(data,binaryValue.getBegin()));
//                    stmt.setInt(i,NumberUtil.fourLEByte2Int(data,binaryValue.getBegin()));
            }else if(binaryValue.getType()== MysqlType.MYSQL_TYPE_SHORT
                    ||binaryValue.getType()== MysqlType.MYSQL_TYPE_YEAR){
                buf.append(NumberUtil.twoLEByte2Int(data,binaryValue.getBegin()));
//                    stmt.setShort(i,(short) NumberUtil.twoLEByte2Int(data,binaryValue.getBegin()));
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_LONGLONG){
                buf.append(NumberUtil.eightLEByteToLong(data,binaryValue.getBegin()));
//                    stmt.setLong(i,NumberUtil.eightLEByteToLong(data,binaryValue.getBegin()));
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_FLOAT){
                buf.append(NumberUtil.byte2Float(data,binaryValue.getBegin()));
//                    stmt.setFloat(i,NumberUtil.byte2Float(data,binaryValue.getBegin()));
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_DOUBLE){
                buf.append(NumberUtil.byte2Double(data,binaryValue.getBegin()));
//                    stmt.setDouble(i,NumberUtil.byte2Double(data,binaryValue.getBegin()));
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_TINY){
                buf.append(data[binaryValue.getBegin()]);
//                    stmt.setByte(i,data[binaryValue.getBegin()]);
            }else if (binaryValue.getType()==MysqlType.MYSQL_TYPE_NEWDECIMAL){
                buf.append(new String(data,binaryValue.getBegin(),binaryValue.getLength()));
//                    stmt.setBigDecimal(i,new BigDecimal(new String(data,binaryValue.getBegin(),binaryValue.getLength())));
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_DATE
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_DATETIME
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_TIMESTAMP
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_DATETIME2){
                int index=binaryValue.getBegin();
                int year=0;
                int month=0;
                int day=0;
                int hour=0;
                int minute=0;
                int second=0;
                int microSecond=0;
                if(binaryValue.getLength()>=4){
                    year=NumberUtil.twoLEByte2Int(data,index);
                    index+=2;
                    month=NumberUtil.oneByte2Int(data,index);
                    index++;
                    day=NumberUtil.oneByte2Int(data,index);
                    index++;
                }

                if(binaryValue.getLength()>=7){
                    hour=NumberUtil.oneByte2Int(data,index);
                    index++;
                    minute=NumberUtil.oneByte2Int(data,index);
                    index++;
                    second=NumberUtil.oneByte2Int(data,index);
                    index++;
                }
                if(binaryValue.getLength()>=11) {
                    microSecond=NumberUtil.fourLEByte2Int(data,index);
                }
                String dateStr=String.format("%04d-%02d-%02d %02d:%02d:%02d.%03d",
                        year,month,day,hour,minute,second,microSecond/1000);
                buf.append("'");
                buf.append(dateStr);
                buf.append("'");
//                    stmt.setString(i,dateStr);

            }else if (binaryValue.getType()==MysqlType.MYSQL_TYPE_TIME){
                int index=binaryValue.getBegin();
                String positive="+";
                int day=0;
                int hour=0;
                int minute=0;
                int second=0;
                int microSecond=0;
                if(binaryValue.getLength()>=4){
                    int tmp=NumberUtil.oneByte2Int(data,index);
                    if(tmp==1){
                        positive="-";
                    }
                    index++;
                    day=NumberUtil.fourLEByte2Int(data,index);
                    index+=4;
                }

                if(binaryValue.getLength()>=8){
                    hour=NumberUtil.oneByte2Int(data,index);
                    index++;
                    minute=NumberUtil.oneByte2Int(data,index);
                    index++;
                    second=NumberUtil.oneByte2Int(data,index);
                    index++;
                }
                if(binaryValue.getLength()>=12) {
                    microSecond=NumberUtil.fourLEByte2Int(data,index);
                }
                String dateStr=String.format("%s%02d %02d:%02d:%02d.%03d",
                        positive,day,hour,minute,second,microSecond/1000);
                buf.append("'");
                buf.append(dateStr);
                buf.append("'");
//                    stmt.setString(i,dateStr);
            } else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_BLOB
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_LONG_BLOB
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_MEDIUM_BLOB
                    ||binaryValue.getType()==MysqlType.MYSQL_TYPE_TINY_BLOB){
                logger.error("not support blob type");
                return null;
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_NULL) {
                buf.append("null");
//                    stmt.setNull(i, 0);
            }
            else{
                logger.error("unknown type:"+binaryValue.getType());
                return null;
            }
            i++;
        }
        while((i-1)<statSql.length){
           buf.append(new String(statSql[i-1],charset));
           i++;
        }
        return buf.toString();
//        }catch (Exception e){
//            logger.error("parse stmt_execute args failed",e);
//        }
    }

    //重连
    public void reconnect() throws SQLException{
        try {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }catch (Exception e){
            logger.warn("jdbc 关闭连接出错:",e);
        }
        initJDBC(this.url,this.username,this.pwd,this.schemaName);
    }

    public void keepAlive()throws SQLException{
        if(!testAlive()){
            reconnect();
        }

    }

    private boolean isEscapeNeededForString(String x, int stringLength) {
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
    //进程启动时初始化一次prepare statement信息
    public void initPSInfoOnce(HashMap<Integer, SqlConsumerThread> consumerThreadMap) throws SQLException, IOException {
        String sql="select a.statement_id,a.sql_text,c.db,c.host from performance_schema.prepared_statements_instances a inner join performance_schema.threads b on a.owner_thread_id=b.thread_id inner join information_schema.processlist c on b.processlist_id=c.id";
        ResultSet resultSet=stmt.executeQuery(sql);
        byte[] header=new byte[]{0x21,0x00,0x00,0x00,0x16};
        while(resultSet.next()){
            long statementId=resultSet.getLong("statement_id");
            String stmtSql=resultSet.getString("sql_text");
            String host=resultSet.getString("host");
            byte[] stmtSqlByte=stmtSql.getBytes(this.charset);
            byte[] prepareData=new byte[stmtSqlByte.length+header.length];
            int i=0;
            while(i<prepareData.length){
                if(i<header.length){
                    prepareData[i]=header[i];
                }else{
                    prepareData[i]=stmtSqlByte[i-5];
                }
                i++;
            }
            ParseInfo parseInfo=new ParseInfo(stmtSql,((JdbcConnection)conn).getSession(),this.charset);
            byte[][] x=parseInfo.getStaticSql();

            MysqlPrepareStmtResponse response=new MysqlPrepareStmtResponse();
            response.setPrepareData(prepareData);
            response.setStatementId((int)statementId);
//            PreparedStatement pstmt=conn.prepareStatement(stmtSql);
//            response.setNumParams( pstmt.getParameterMetaData().getParameterCount());
//            pstmt.close();
            if(x.length==0){
                response.setNumParams(0);
            }else{
                response.setNumParams(x.length-1);
            }


            Integer outKey = TCPPacketUtil.getThreadKey(host);
            SqlConsumerThread outConsumerThread=consumerThreadMap.get(outKey);
            LinkedHashMap<String, MysqlPrepareStmtResponse> prepareStmtResponseMap=outConsumerThread.getPrepareStmtInfoMap();
            prepareStmtResponseMap.put(TCPPacketUtil.getPrepareStmtResponseKey(host,(int)statementId),response);

        }
    }

    public boolean generalLogOpened() throws SQLException{
        String sql="show variables like 'general_log'";
        ResultSet resultSet= stmt.executeQuery(sql);
        resultSet.next();
        String value=resultSet.getString("Value");
        return value.equals("ON");
    }

    public String getGeneralLogPath() throws SQLException{
        String sql="show variables like 'general_log_file'";
        ResultSet resultSet= stmt.executeQuery(sql);
        resultSet.next();
        String value=resultSet.getString("Value");
        return value;
    }

}
