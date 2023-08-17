package com.aliyun.gts.sniffer.mypcap;

import com.aliyun.gts.sniffer.common.entity.MysqlPrepareStmtResponse;
import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mysql.MysqlCmdType;
import jpcap.PacketReceiver;
import jpcap.packet.Packet;
import jpcap.packet.TCPPacket;
import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoke on 17/10/28.
 * author:zhaoke
 * email:zhkeke2008@163.com
 */
public class MysqlPacketHandlerDeleted implements PacketReceiver {
    private Logger logger= org.slf4j.LoggerFactory.getLogger(MysqlPacketHandlerDeleted.class);
    //用来缓存小包以及合并过的包
    private final int maxPacketQueueSize=655350;
    private ArrayBlockingQueue<TCPPacket> packetQueue=new ArrayBlockingQueue<TCPPacket>(maxPacketQueueSize);



    //用来缓存大包,生产者消费者模式,提供给merge线程消费
    private final int maxLargePacketQueueSize=102400;
    private ArrayBlockingQueue<TCPPacket> largePacketQueue=new ArrayBlockingQueue<TCPPacket>(maxLargePacketQueueSize);

    //用于同步更新maxLargePacketMapSize
    private Object maxLargePacketMapSizeObjectLock=new Object();

    //merge线程中大包缓存最大数量
    private final int maxLargePacketMapSize=65535;
    //merge线程会缓存大包,然后每次新包进来,都会触发merge任务,尝试合并为大包,key设计为tcp.ack_num
    private ConcurrentHashMap<Long,MysqlMultiPacket> largePacketMap=new ConcurrentHashMap<Long, MysqlMultiPacket>(maxLargePacketMapSize);
    //merge线程中缓存的大包数量
   // private int largePacketMapSize=0;
    private AtomicInteger largePacketMapSize=new AtomicInteger(0);
    private MysqlProcessListMeta meta=MysqlProcessListMeta.getInstance();

    //用于缓存哪些源ip port是prepare stmt,以及对应的sql
    private final int maxPrepareIpPortMapSize=65535;
    private ConcurrentHashMap<String,TCPPacket> prepareIpPort=new ConcurrentHashMap<>(maxPrepareIpPortMapSize);

    //用于缓存prepare stmt 的statement_id、num_params等信息
    private LinkedHashMap<String, MysqlPrepareStmtResponse> prepareStmtInfoMap=new LinkedHashMap<>(Config.maxPrepareStmtCacheSize);

    public MysqlPrepareStmtResponse getMysqlPrepareStmtResponse(String ip,int port,int stmtId){
        return prepareStmtInfoMap.get(ip+":"+port+":"+stmtId);
    }



//    //
//    private long abandedLargePacketCnt=0l;
//    private long abandedPacketCnt=0l;


    public ConcurrentHashMap<Long, MysqlMultiPacket> getLargePacketMap() {
        return largePacketMap;
    }

    public ArrayBlockingQueue<TCPPacket> getPacketQueue() {
        return packetQueue;
    }

    public int getLargePacketMapSize() {
        return largePacketMapSize.intValue();
    }

    public int getMaxLargePacketMapSize() {
        return maxLargePacketMapSize;
    }

    public void largeIncr(){
        largePacketMapSize.getAndIncrement();
    }

    public void largeDecr(){
        largePacketMapSize.getAndDecrement();
    }

    public boolean isLargeMapFull(){
        synchronized (maxLargePacketMapSizeObjectLock){
            if (largePacketMapSize.intValue()>=maxLargePacketMapSize){
                return true;
            }else{
                return false;
            }
        }
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

    public  boolean isLargeQueueFull(){
        if (largePacketQueue.size()>=maxLargePacketQueueSize){
            return true;
        }else{
            return false;
        }
    }

    //如果queue 满了,那么直接丢掉该包
    public void packetQueueAdd(TCPPacket tcp){
        packetQueue.offer(tcp);
    }

    //如果large queue 满了,那么直接丢掉该包
    public void largePacketQueueAdd(TCPPacket tcp){
        largePacketQueue.offer(tcp);
    }

    public  TCPPacket get() throws InterruptedException{
        TCPPacket tcp=packetQueue.take();
        return tcp;
    }

    public void receivePacket(Packet arg0) {
        if (arg0 instanceof TCPPacket && ((TCPPacket) arg0).version == 4) {
            TCPPacket tcp = (TCPPacket) arg0;//强转
//            System.out.println(new String(tcp.data));
            if(tcp.src_port==Config.getPort()){
                //分包的直接忽略，只抓取execute_stmt返回的OK包，OK包很小，不会涉及到分包
                if(!tcp.psh){
                    return;
                }
                if(!prepareIpPort.containsKey(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port)){
                    return;
                }
                //判断status是否是0x00
                if(tcp.data[0]==0x0c && tcp.data[1]==0x00 && tcp.data[2]==0x00 && tcp.data[3]==0x01&&tcp.data[4]==0x00){
                    MysqlPrepareStmtResponse stmtResponse=new MysqlPrepareStmtResponse();
                    try{
                        stmtResponse.setStatementId(NumberUtil.fourLEByte2Int(tcp.data,5));
                        stmtResponse.setNumColumns(NumberUtil.twoLEByte2Int(tcp.data,9));
                        stmtResponse.setNumParams(NumberUtil.twoLEByte2Int(tcp.data,11));
                        //装载prepare的sql信息，可能会超过单个包的长度，导致sql截断。
                        stmtResponse.setPrepareData(prepareIpPort.get(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port).data);
                        //stmt信息装载到LRU链表
                        prepareStmtInfoMap.put(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port+":"+stmtResponse.getStatementId(),stmtResponse);
                        return;
                    }catch (Exception e){
                        logger.error("packet prepare stmt response error",e);
                    }
                }
                prepareIpPort.remove(tcp.dst_ip.getHostAddress()+":"+tcp.dst_port);
            }

            //目标端口为本机监听端口,并且包的目标机器IP不能等于要预热的机器IP
            if(tcp.dst_port== Config.getPort() && !tcp.dst_ip.getHostAddress().equals(Config.getDstIp())){
                byte[] data=tcp.data;
                if(data.length>5 ){
                   // System.out.println(new String(tcp.data));

                    //检查是否是登陆鉴权包
                    //psh必须是true，tcp.data[4]=0x0f
                    if(tcp.psh && tcp.data[3]== MysqlCmdType.COM_QUIT && tcp.data.length>10 &&!meta.exists(tcp.src_ip.getHostAddress(),tcp.src_port)){
                        //如果是登陆鉴权包,那么要更新连接元信息
                        if(login(tcp)){
                            return;
                        }
                    }

                    //如果队列满了,直接返回,丢弃packet
                    if(isFull()==true){
                        return;
                    }
                    byte type= data[4];
                    if (type==MysqlCmdType.COM_INIT_DB){
                        meta.updateHost(tcp.src_ip.getHostAddress(),tcp.src_port,null,new String(tcp.data,5,tcp.data.length-5));
                    }
                    if( tcp.psh &&(type== MysqlCmdType.COM_QUERY || type== MysqlCmdType.COM_STMT_EXECUTE || type==MysqlCmdType.COM_STMT_PREPARE)){
                        //维护prepareIpPort 列表，用于检测哪些ip port 正在进行prepare stmt
                        if(type==MysqlCmdType.COM_STMT_PREPARE){
                            prepareIpPort.put(tcp.src_ip.getHostAddress()+":"+tcp.src_port,tcp);
                        }
                        if(type==MysqlCmdType.COM_STMT_EXECUTE){
                            System.out.println(tcp.data.length);
                        }
                        packetQueueAdd(tcp);
                    } else {
                        largePacketQueueAdd(tcp);
                    }
                }

            }

        }

    }

    //大包消费者,等待消费
    public void takeLargePacket() throws InterruptedException{
        TCPPacket tcp=largePacketQueue.take();
        mergeLargePacket(tcp);
    }


    public void mergeLargePacket(TCPPacket tcp){
        TCPPacket dstPacket=null;
        //超过1444长度的需要合包,因为大包合包比较慢,考虑单独开一个线程处理
        if(largePacketMap.containsKey(tcp.ack_num)){
            MysqlMultiPacket mysqlMultiPacket=largePacketMap.get(tcp.ack_num);
            dstPacket=mysqlMultiPacket.addPacket(tcp);
            if(dstPacket!=null){
                largePacketMap.remove(tcp.ack_num);
                largeDecr();
            }else{
                return;
            }
        }else{
            MysqlMultiPacket mysqlMultiPacket=new MysqlMultiPacket();
            dstPacket=mysqlMultiPacket.addPacket(tcp);
            //如果刚刚好是完整的一个包,那么不用合包
            if(dstPacket==null){
                if(!isLargeMapFull()){
                    largePacketMap.put(tcp.ack_num,mysqlMultiPacket);
                    largeIncr();
                }
                return;
            }
        }
        packetQueueAdd(dstPacket);
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


}
