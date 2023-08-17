package com.aliyun.gts.sniffer.mypcap;

import com.aliyun.gts.sniffer.common.entity.MysqlPrepareStmtResponse;
import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import com.aliyun.gts.sniffer.common.utils.TCPPacketUtil;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mysql.MysqlCmdType;
import com.aliyun.gts.sniffer.thread.LargePacketMergeThread;
import com.aliyun.gts.sniffer.thread.SqlConsumerThread;
import com.aliyun.gts.sniffer.thread.SqlConsumerThreadDeleted;
import jpcap.PacketReceiver;
import jpcap.packet.Packet;
import jpcap.packet.TCPPacket;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoke on 17/10/28.
 * author:zhaoke
 * email:zhkeke2008@163.com
 */
public class MysqlPacketHandler implements PacketReceiver {
    private Logger logger= org.slf4j.LoggerFactory.getLogger(MysqlPacketHandler.class);
    //用来缓存小包以及合并过的包
    private final int maxPacketQueueSize=655350;
    private ArrayBlockingQueue<TCPPacket> packetQueue=new ArrayBlockingQueue<TCPPacket>(maxPacketQueueSize);

    private HashMap<Integer, SqlConsumerThread> consumerThreadMap;

    private HashMap<Integer, LargePacketMergeThread> largePacketMergeThreadMap;

    //merge线程中大包缓存最大数量
    private final int maxLargePacketMapSize=65535;
    //merge线程会缓存大包,然后每次新包进来,都会触发merge任务,尝试合并为大包,key设计为tcp.ack_num
    private ConcurrentHashMap<Long,MysqlMultiPacket> largePacketMap=new ConcurrentHashMap<Long, MysqlMultiPacket>(maxLargePacketMapSize);
    //merge线程中缓存的大包数量
   // private int largePacketMapSize=0;
    private AtomicInteger largePacketMapSize=new AtomicInteger(0);
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();

    public MysqlPacketHandler(HashMap<Integer, SqlConsumerThread> consumerThreadMap, HashMap<Integer, LargePacketMergeThread> largePacketMergeThreadMap){
        this.consumerThreadMap=consumerThreadMap;
        this.largePacketMergeThreadMap=largePacketMergeThreadMap;
    }

    public int getLargePacketMapSize() {
        return largePacketMapSize.intValue();
    }

    public int getMaxLargePacketMapSize() {
        return maxLargePacketMapSize;
    }


    public long getMaxPacketQueueSize(){
        return this.maxPacketQueueSize;
    }
    public long getPacketQueueSize(){
        return packetQueue.size();
    }

    public  boolean isFull(){
        if (packetQueue.size()>=maxPacketQueueSize){
            return true;
        }else{
            return false;
        }
    }

    public  TCPPacket get() throws InterruptedException{
        TCPPacket tcp=packetQueue.take();
        return tcp;
    }

    public void receivePacket(Packet arg0) {
        if (arg0 instanceof TCPPacket && ((TCPPacket) arg0).version == 4) {
            TCPPacket tcp = (TCPPacket) arg0;//强转

//            System.out.println(new String(tcp.data));
            //回包分析，主要找prepare_statement_ok包
            if(tcp.src_port==Config.getPort() && tcp.dst_port!=Config.getPort()){
                //分包的直接忽略，只抓取execute_stmt返回的OK包，OK包很小，不会涉及到分包
                if(!tcp.psh){
                    return;
                }
                Integer outKey = TCPPacketUtil.getThreadKey(tcp.dst_ip.getHostAddress(),tcp.dst_port);
                SqlConsumerThread outConsumerThread=consumerThreadMap.get(outKey);
                HashMap<String,TCPPacket> outPrepareIpPort=outConsumerThread.getPrepareIpPort();
                LinkedHashMap<String, MysqlPrepareStmtResponse> prepareStmtInfoMap=outConsumerThread.getPrepareStmtInfoMap();
                if(!outPrepareIpPort.containsKey(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port)){
                    return;
                }
                //判断是否是prepare_statement_ok包
                if(tcp.data[0]==0x0c && tcp.data[1]==0x00 && tcp.data[2]==0x00 && tcp.data[3]==0x01&&tcp.data[4]==0x00){
                    MysqlPrepareStmtResponse stmtResponse=new MysqlPrepareStmtResponse();
                    try{
                        stmtResponse.setStatementId(NumberUtil.fourLEByte2Int(tcp.data,5));
                        stmtResponse.setNumColumns(NumberUtil.twoLEByte2Int(tcp.data,9));
                        stmtResponse.setNumParams(NumberUtil.twoLEByte2Int(tcp.data,11));
                        //装载prepare的sql信息，可能会超过单个包的长度，导致sql截断。
                        stmtResponse.setPrepareData(outPrepareIpPort.get(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port).data);
                        //stmt信息装载到LRU链表
                        prepareStmtInfoMap.put(TCPPacketUtil.getPrepareStmtResponseKey(tcp.dst_ip.getHostAddress(),tcp.dst_port,stmtResponse.getStatementId()),stmtResponse);
                        return;
                    }catch (Exception e){
                        logger.error("packet prepare stmt response error",e);
                    }
                }
                outPrepareIpPort.remove(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port);
            }

            Integer key = TCPPacketUtil.getThreadKey(tcp.src_ip.getHostAddress(),tcp.src_port);
            SqlConsumerThread consumerThread=consumerThreadMap.get(key);

            //目标端口为本机监听端口,并且包的目标机器IP不能等于要预热的机器IP
            if(tcp.dst_port== Config.getPort() && !tcp.dst_ip.getHostAddress().equals(Config.getDstIp())){
                byte[] data=tcp.data;
                if(data.length>5 ){
//                    System.out.println(new String(tcp.data));
                    //鉴权包
                    if(tcp.psh && tcp.data[3]== MysqlCmdType.COM_QUIT && tcp.data.length>10 &&!meta.exists(tcp.src_ip.getHostAddress(),tcp.src_port)){
                        consumerThread.packetQueueAdd(tcp);
                        return;
                    }
                    //如果队列满了,直接返回,丢弃packet
                    if(consumerThread.isFull()){
                        return;
                    }
                    byte type= data[4];
                    if (type==MysqlCmdType.COM_INIT_DB){
                        consumerThread.packetQueueAdd(tcp);
                        return;
                    }
                    if( tcp.psh &&(type== MysqlCmdType.COM_QUERY || type== MysqlCmdType.COM_STMT_EXECUTE || type==MysqlCmdType.COM_STMT_PREPARE)){
                        consumerThread.packetQueueAdd(tcp);
                    } else {
                        LargePacketMergeThread largePacketMergeThread=largePacketMergeThreadMap.get(key);
                        largePacketMergeThread.largePacketQueueAdd(tcp);
                    }
                }

            }

        }

    }

}
