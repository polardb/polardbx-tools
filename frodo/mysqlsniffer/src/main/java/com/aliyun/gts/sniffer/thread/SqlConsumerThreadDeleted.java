package com.aliyun.gts.sniffer.thread;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.MysqlBinaryValue;
import com.aliyun.gts.sniffer.common.entity.MysqlPrepareStmtResponse;
import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlPacketHandlerDeleted;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.aliyun.gts.sniffer.mysql.MysqlCmdType;
import com.aliyun.gts.sniffer.mysql.MysqlType;
import jpcap.packet.TCPPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by zhaoke on 17/10/30.
 */
public class SqlConsumerThreadDeleted extends Thread {
    private MysqlPacketHandlerDeleted mysqlPacketHandler;
    private Logger logger= LoggerFactory.getLogger(SqlConsumerThreadDeleted.class);
    private JDBCUtils jdbcUtils=null;
    private JDBCUtils sourceJdbcUtils=null;
    private volatile boolean isRunning=true;
    private MysqlProcessListMeta mysqlProcessListMeta;
    //缓存的packet数量
    private ArrayBlockingQueue<TCPPacket> packetQueue=new ArrayBlockingQueue<TCPPacket>(Config.maxPacketQueueSize);

    public void packetQueueAdd(TCPPacket packet){
        packetQueue.offer(packet);
    }

    public boolean isFull(){
        return packetQueue.size()==Config.maxPacketQueueSize;
    }


    //用于缓存哪些源ip port是prepare stmt,以及对应的sql
    private final int maxPrepareIpPortMapSize=65535;
    private HashMap<String,TCPPacket> prepareIpPort=new HashMap<>(maxPrepareIpPortMapSize);

    //用于缓存prepare stmt 的statement_id、num_params等信息
    private LinkedHashMap<String, MysqlPrepareStmtResponse> prepareStmtInfoMap=new LinkedHashMap<>(Config.maxPrepareStmtCacheSize);

    private MysqlPrepareStmtResponse getMysqlPrepareStmtResponse(String ip,int port,int stmtId){
        return prepareStmtInfoMap.get(ip+":"+port+":"+stmtId);
    }

    public HashMap<String,TCPPacket> getPrepareIpPort(){
        return this.prepareIpPort;
    }


    public SqlConsumerThreadDeleted(MysqlPacketHandlerDeleted mysqlPacketHandler) throws SQLException{
        this.mysqlPacketHandler=mysqlPacketHandler;
        String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql";
        String sourceUrl="jdbc:mysql://127.0.0.1:"+Config.getPort()+"/mysql";
        jdbcUtils=new JDBCUtils(url,Config.getDstUsername(), Config.getDstPassword(),"mysql");
        sourceJdbcUtils=new JDBCUtils(sourceUrl,Config.getUsername(), Config.getPassword(),"mysql");
        mysqlProcessListMeta=MysqlProcessListMeta.getInstance();
    }


    public void close(){
        isRunning=false;
    }
    @Override
    public void run(){
        while(isRunning){
            try{
                TCPPacket tcpPacket=mysqlPacketHandler.get();
                applyMysqlPacket(tcpPacket);
                tcpPacket=null;
            }catch (InterruptedException e){
                isRunning=false;
                break;
            }
        }
        jdbcUtils.close();
    }
    private void applyMysqlPacket(TCPPacket tcpPacket){
        int index=0;
        String sql=null;
        int length=tcpPacket.data.length-5;
        //offset 4 命令标记位
        index=4;
        byte type=tcpPacket.data[index++];
        ProcessModel processModel=null;
        processModel=mysqlProcessListMeta.getProcessModelByHost(tcpPacket.src_ip.getHostAddress().toString(),tcpPacket.src_port);
        if(type==MysqlCmdType.COM_QUERY){
            sql=new String(tcpPacket.data,index,length);
        } else if(type==MysqlCmdType.COM_STMT_EXECUTE){
            int stmtId= NumberUtil.fourLEByte2Int(tcpPacket.data,index);
            index=index+4;
            MysqlPrepareStmtResponse stmtResponse= mysqlPacketHandler.getMysqlPrepareStmtResponse(tcpPacket.src_ip.getHostAddress(),tcpPacket.src_port,stmtId);
            if(stmtResponse==null){
                return;
            }
            String stmtSql=stmtResponse.getStmtSql();
            try{
                //todo:解析 execute传入的参数
                List<MysqlBinaryValue> binaryValueList=resolveStmt(tcpPacket,stmtResponse);
                //clob blob不支持
                if(binaryValueList==null){
                    logger.error(String.format("transfer stmt sql failed,type not support,stmt sql:%s",stmtSql));
                    return;
                }
                sql=sourceJdbcUtils.toSqlStr(stmtSql,binaryValueList);
            }catch (Exception e){
                logger.error(String.format("transfer stmt sql failed,stmt sql:%s",stmtSql), e);
            }
        }else {
            return;
        }
        if(!StringUtils.isEmpty(sql)){
            if(Config.filterSelect && !sql.toLowerCase(Locale.ROOT).startsWith("select")){
                return;
            }
//            logger.info(getSqlJSON(sql,processModel).toJSONString());
        }else{
            logger.error("sql parse failed");
        }


        logger.info(sql);
        try{
//            MySqlStatementParser parser = new MySqlStatementParser(sql);
//            List<SQLSelectStatement> statementList=parser.parseStatementList();
//            for (SQLSelectStatement sqlStatement:statementList){
//                if(sqlStatement instanceof SQLSelectStatement){
//                    //如果DB为null,说明找不到该连接,忽略该sql
                    if(processModel==null){
                        return;
                        //如果连接的DB值不为null,那么需要切换连接到当前的DB,use db
                    }else if(!processModel.getDB().equals("")){
                        try{
                            jdbcUtils.switchDB(processModel.getDB());
                        }catch (SQLException e){
                            return;
                        }
                    }
                    for (int i=0;i<Config.getEnlarge();i++){
                        jdbcUtils.execute(sql);
                    }
//                }
//            }

        }catch (Exception e){
            logger.error("sql apply failed,sql:"+sql,e);
        }
    }
    private String getSelectStr(SQLSelectStatement sqlSelectStatement){
        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        sqlSelectStatement.accept(visitor);
        return out.toString();
    }

    private JSONObject getSqlJSON(String sql,ProcessModel processModel){
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("sql",sql);
        jsonObject.put("sqlId",toSqlId(sql));
        if(processModel!=null){
            jsonObject.put("db",processModel.getDB());
            jsonObject.put("user",processModel.getUser());
        }else {
            jsonObject.put("db","");
            jsonObject.put("user","");
        }
        return jsonObject;
    }

    private List<MysqlBinaryValue> resolveStmt(TCPPacket tcpPacket,MysqlPrepareStmtResponse stmtResponse){
        int index;
        ArrayList<MysqlBinaryValue> valueList=new ArrayList<>();
        if(stmtResponse.getNumParams()<=0){
            return valueList;
        }
        byte[] nullBitmap;
        int nullBitmapLength=(stmtResponse.getNumParams()+7)/8;
        //nullbitmap的起点
        index=14;
        nullBitmap= Arrays.copyOfRange(tcpPacket.data,index,index+nullBitmapLength);
        index=index+nullBitmapLength;
        int newParamsBoundFlag= NumberUtil.oneByte2Int(tcpPacket.data,index);
        index++;
        if(newParamsBoundFlag!=1){
            return valueList;
        }
        for(int i=0;i<stmtResponse.getNumParams();i++){
            MysqlBinaryValue binaryValue=new MysqlBinaryValue();
            binaryValue.setType(NumberUtil.twoLEByte2Int(tcpPacket.data,index));
            binaryValue.setData(tcpPacket.data);
            index=index+2;
            valueList.add(binaryValue);
        }

        /*
         *  fixed 类型没有长度标志位，只有不定长度类型需要长度标志位
         *
         */
        for(int i=0;i<stmtResponse.getNumParams();i++){
            MysqlBinaryValue binaryValue=valueList.get(i);
            if(isBitmapNull(nullBitmap,i)){
                continue;
            }
            //setString
            if(binaryValue.getType()== MysqlType.MYSQL_TYPE_STRING
                    || binaryValue.getType()== MysqlType.MYSQL_TYPE_VAR_STRING){
                int intLen= NumberUtil.lengthInt(tcpPacket.data[index]);
                long len= NumberUtil.resolveEncodedInteger(tcpPacket.data,index);
                int begin=0;
                if(intLen==1){
                    begin=index+1;
                }else{
                    begin=index+intLen+1;
                }
                index=(int)(begin+len);
                binaryValue.setBegin(begin);
                binaryValue.setLength((int)len);
            }else if(binaryValue.getType()== MysqlType.MYSQL_TYPE_LONG
                ||binaryValue.getType()== MysqlType.MYSQL_TYPE_INT24){
                binaryValue.setBegin(index);
                index=index+4;
                binaryValue.setLength(4);
            }else if(binaryValue.getType()== MysqlType.MYSQL_TYPE_SHORT
                ||binaryValue.getType()== MysqlType.MYSQL_TYPE_YEAR){
                binaryValue.setBegin(index);
                index=index+2;
                binaryValue.setLength(2);
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_LONGLONG){
                binaryValue.setBegin(index);
                index=index+8;
                binaryValue.setLength(8);
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_FLOAT){
                binaryValue.setBegin(index);
                index = index + 4;
                binaryValue.setLength(4);
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_DOUBLE){
                binaryValue.setBegin(index);
                index = index + 8;
                binaryValue.setLength(8);
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_TINY){
                binaryValue.setBegin(index);
                index = index + 1;
                binaryValue.setLength(1);
            }else if (binaryValue.getType()==MysqlType.MYSQL_TYPE_NEWDECIMAL){
                //NEWDECIMAL 传递的是字符串
                int intLen= NumberUtil.lengthInt(tcpPacket.data[index]);
                long len= NumberUtil.resolveEncodedInteger(tcpPacket.data,index);
                int begin=0;
                if(intLen==1){
                    begin=index+1;
                }else{
                    begin=index+intLen+1;
                }
                index=(int)(index+intLen+len);
                binaryValue.setBegin(begin);
                binaryValue.setLength((int)len);
            }else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_DATE
                ||binaryValue.getType()==MysqlType.MYSQL_TYPE_DATETIME
                ||binaryValue.getType()==MysqlType.MYSQL_TYPE_TIMESTAMP
                ||binaryValue.getType()==MysqlType.MYSQL_TYPE_DATETIME2){
                //第一位是长度标记位 0 4 7 11 4种长度
                int len=NumberUtil.oneByte2Int(tcpPacket.data,index);
                index++;
                int begin=index;
                index=index+len;
                binaryValue.setBegin(begin);
                binaryValue.setLength(len);
            }else if (binaryValue.getType()==MysqlType.MYSQL_TYPE_TIME){
                //第一位是长度标记位 0 8 12 3种长度,第二位是正负数
                int len=NumberUtil.oneByte2Int(tcpPacket.data,index);
                index++;
                int begin=index;
                index=index+len;
                binaryValue.setBegin(begin);
                binaryValue.setLength(len);
            }
            else if(binaryValue.getType()==MysqlType.MYSQL_TYPE_BLOB
            ||binaryValue.getType()==MysqlType.MYSQL_TYPE_LONG_BLOB
            ||binaryValue.getType()==MysqlType.MYSQL_TYPE_MEDIUM_BLOB
            ||binaryValue.getType()==MysqlType.MYSQL_TYPE_TINY_BLOB){
                logger.error("not support blob type");
                return null;
            } else{
                logger.error("unknown type:"+binaryValue.getType());
                return null;
            }

        }
        return valueList;
    }
    private boolean isBitmapNull(byte[] nullBitmap,int args){
        int flag=args/8;
        int nullBitmapValue= NumberUtil.oneByte2Int(nullBitmap,flag);
        int oneByteOffset=args%8;
        int compareByte=1<<oneByteOffset;
        if((nullBitmapValue & compareByte)==compareByte){
            return true;
        }
        return false;
    }

    //返回sql参数化后的唯一标识
    private String toSqlId(String sql){
        String dbtype = JdbcConstants.MYSQL;
        String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
        Integer hashcode=fs.hashCode();
        return String.format("%x",new BigInteger(1,hashcode.toString().getBytes(StandardCharsets.UTF_8)));
    }
}


