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
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import com.aliyun.gts.sniffer.common.utils.TCPPacketUtil;
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

public class SqlConsumerThread extends ConsumerThread {
    private Logger logger= LoggerFactory.getLogger(SqlConsumerThread.class);
    //缓存的packet数量
    private ArrayBlockingQueue<TCPPacket> packetQueue=new ArrayBlockingQueue<TCPPacket>(Config.maxPacketQueueSize);

    public void packetQueueAdd(TCPPacket packet){
        if(packet.data.length>5){
            byte type=packet.data[4];
            if(type==MysqlCmdType.COM_STMT_PREPARE){
                prepareIpPort.put(packet.src_ip.getHostAddress()+":"+packet.src_port,packet);
                return;
            }
        }

        packetQueue.offer(packet);
    }

    //用于缓存哪些源ip port是prepare stmt,以及对应的sql
    private HashMap<String,TCPPacket> prepareIpPort=new HashMap<>(65535);
    //processlist维护列表
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();

    //用于缓存prepare stmt 的statement_id、num_params等信息
    private LinkedHashMap<String, MysqlPrepareStmtResponse> prepareStmtInfoMap=new LinkedHashMap<>(65535);

    public LinkedHashMap<String, MysqlPrepareStmtResponse> getPrepareStmtInfoMap(){
        return prepareStmtInfoMap;
    }

    private MysqlPrepareStmtResponse getMysqlPrepareStmtResponse(String ip,int port,int stmtId){
        return prepareStmtInfoMap.get(TCPPacketUtil.getPrepareStmtResponseKey(ip,port,stmtId));
    }

    public HashMap<String,TCPPacket> getPrepareIpPort(){
        return this.prepareIpPort;
    }

    public boolean isFull(){
        return packetQueue.size()==Config.maxPacketQueueSize;
    }


    //处理login包,获取连接的db  username
    private boolean login(TCPPacket tcp){
        try{
            int start=36;
            byte x=tcp.data[start];
            int stop=start;
            while(x!=0x00){
                stop++;
                x=tcp.data[stop];
            }
            String username=new String(tcp.data,start,stop-start);
            int passwordLength=tcp.data[++stop];
            stop=stop+passwordLength;
            String db=null;
            //判断是否connect with database
            //with database
            if((tcp.data[4] & 0x08)==0x08){
                start=++stop;
                x=tcp.data[stop];
                while(x!=0x00){
                    stop++;
                    x=tcp.data[stop];
                }
                db=new String(tcp.data,start,stop-start);
            }
             meta.updateHost(tcp.src_ip.getHostAddress(),tcp.src_port,username,db);

        }catch (Exception e){

        }
        return true;
    }


    public SqlConsumerThread() throws SQLException{
        if(Config.replayTo.equals("mysql")){
            String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql?allowPublicKeyRetrieval=true";
//        String sourceUrl="jdbc:mysql://127.0.0.1:"+Config.getPort()+"/mysql?allowPublicKeyRetrieval=true";
            jdbcUtils=new JDBCUtils(url,Config.getDstUsername(), Config.getDstPassword(),"mysql");
//        sourceJdbcUtils=new JDBCUtils(sourceUrl,Config.getUsername(), Config.getPassword(),"mysql");
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
                TCPPacket tcpPacket=packetQueue.take();
                applyMysqlPacket(tcpPacket);
                tcpPacket=null;
            }catch (InterruptedException e){
                isRunning=false;
                break;
            }
        }
        //jdbcUtils.close();
    }
    private void applyMysqlPacket(TCPPacket tcpPacket){
        int index=0;
        List<String> sqlList=new ArrayList<>();
        int length=tcpPacket.data.length-5;
        //offset 4 命令标记位
        index=4;
        byte type=tcpPacket.data[index++];
        ProcessModel processModel=null;
        processModel=mysqlProcessListMeta.getProcessModelByHost(tcpPacket.src_ip.getHostAddress(),tcpPacket.src_port);
        //检查是否是登录鉴权包
        //一般鉴权包一定是app发给数据库，如果出现数据库发给app，那么tcp目标端口和数据库端口一般不一致
        if(tcpPacket.dst_port==Config.port){
            if(tcpPacket.psh && tcpPacket.data[3]== MysqlCmdType.COM_QUIT && tcpPacket.data.length>10 &&!meta.exists(tcpPacket.src_ip.getHostAddress(),tcpPacket.src_port)){
                //如果是登陆鉴权包,那么要更新连接元信息
                if(login(tcpPacket)){
                    return;
                }
            }
        }

        if (type==MysqlCmdType.COM_INIT_DB){
            meta.updateHost(tcpPacket.src_ip.getHostAddress(),tcpPacket.src_port,null,new String(tcpPacket.data,5,tcpPacket.data.length-5));
            return;
        }

        if(type==MysqlCmdType.COM_QUERY){
            sqlList.add(new String(tcpPacket.data,index,length));
        } else if(type==MysqlCmdType.COM_STMT_EXECUTE){
            int stmtId= NumberUtil.fourLEByte2Int(tcpPacket.data,index);
            index=index+4;
            MysqlPrepareStmtResponse stmtResponse= getMysqlPrepareStmtResponse(tcpPacket.src_ip.getHostAddress(),tcpPacket.src_port,stmtId);
            //防止有序链表内容太多，造成内存过大
            if(prepareStmtInfoMap.size()>=65535){
                Set<String> set=prepareStmtInfoMap.keySet();
                String first=set.iterator().next();
                if(first!=null)
                prepareStmtInfoMap.remove(first);
            }
            if(stmtResponse==null){
                return;
            }
            String stmtSql=stmtResponse.getStmtSql();
            try{
                int fetchIndex=0;
                while(true){
                    //解析 execute传入的参数
                    List<MysqlBinaryValue> binaryValueList=resolveStmt(tcpPacket,stmtResponse,fetchIndex);
                    //clob blob不支持
                    if(binaryValueList==null){
                        logger.debug(String.format("transfer stmt sql failed,type not support,stmt sql:%s",stmtSql));
                        break;
                    }
                    if(binaryValueList.size()==0){
                        sqlList.add(stmtSql);
                        break;
                    }else{
                        sqlList.add(srcJdbcUtils.toSqlStr(stmtSql,binaryValueList));
                    }
                    MysqlBinaryValue binaryValue=binaryValueList.get(binaryValueList.size()-1);
                    fetchIndex=binaryValue.getBegin()+binaryValue.getLength();
                    if(fetchIndex>=tcpPacket.data.length){
                        break;
                    }
                }
            }catch (Exception e){
                logger.error(String.format("transfer stmt sql failed,stmt sql:%s",stmtSql), e);
                return;
            }
        } else{
            return;
        }

        for(String sql:sqlList){
            //过滤掉commit rollback begin start transaction
            String sql2=sql.toLowerCase(Locale.ROOT);
            if(sql2.startsWith("begin")||sql2.startsWith("commit")
                    ||sql2.startsWith("rollback")||sql2.startsWith("start")){
                continue;
            }

            if(filterDefault(sql2)){
                continue;
            }
            //是否命中需要输出的sql，如果没命中，那么忽略sql
            if(!filterSQL(sql2)){
                continue;
            }
            if(Config.replayTo.equals("stdout")){
                logger.info(sql);
                continue;
            }
            if(Config.replayTo.equals("file")){
                fileResultLogger.info(getSqlJSON(sql,processModel).toJSONString());
                continue;
            }
            if(Config.replayTo.equals("mysql")){
                //如果DB为null,说明找不到该连接,忽略该sql
                if(processModel==null){
                    continue;
                    //如果连接的DB值不为null,那么需要切换连接到当前的DB,use db
                }
                applyMysql(sql,processModel.getDB(),toSqlId(sql));


            }
        }
    }
    private String getSelectStr(SQLSelectStatement sqlSelectStatement){
        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        sqlSelectStatement.accept(visitor);
        return out.toString();
    }

    private List<MysqlBinaryValue> resolveStmt(TCPPacket tcpPacket,MysqlPrepareStmtResponse stmtResponse,int preIndex){
        ArrayList<MysqlBinaryValue> valueList=new ArrayList<>();
        if(stmtResponse.getNumParams()<=0){
            return valueList;
        }
        byte[] nullBitmap;
        int nullBitmapLength=(stmtResponse.getNumParams()+7)/8;
        //nullbitmap的起点
        int index=preIndex+14;
        nullBitmap= Arrays.copyOfRange(tcpPacket.data,index,index+nullBitmapLength);
        index=index+nullBitmapLength;
        int newParamsBoundFlag= NumberUtil.oneByte2Int(tcpPacket.data,index);
        index++;

        if(newParamsBoundFlag==0){
            List<MysqlBinaryValue> preValueList=ConsumerThread.batchTypeCache.get(TCPPacketUtil.getPrepareStmtResponseKey(tcpPacket.dst_ip.getHostAddress(),tcpPacket.dst_port,stmtResponse.getStatementId()));
            if(preValueList==null){
                return null;
            }
            for(MysqlBinaryValue val:preValueList){
                MysqlBinaryValue binaryValue=new MysqlBinaryValue();
                binaryValue.setType(val.getType());
                binaryValue.setData(tcpPacket.data);
                valueList.add(binaryValue);
            }
        }else{
            for(int i=0;i<stmtResponse.getNumParams();i++){
                MysqlBinaryValue binaryValue=new MysqlBinaryValue();
                binaryValue.setType(NumberUtil.twoLEByte2Int(tcpPacket.data,index));
                binaryValue.setData(tcpPacket.data);
                index=index+2;
                valueList.add(binaryValue);
            }
            //Batch写入，会忽略类型信息，需要从第一个请求获取类型信息。
            ConsumerThread.batchTypeCache.put(TCPPacketUtil.getPrepareStmtResponseKey(tcpPacket.dst_ip.getHostAddress(),tcpPacket.dst_port,stmtResponse.getStatementId()),valueList);
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
                    || binaryValue.getType()== MysqlType.MYSQL_TYPE_VAR_STRING
                        ||binaryValue.getType()==MysqlType.MYSQL_TYPE_VARCHAR){
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
}


