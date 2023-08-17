package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.common.utils.TCPPacketUtil;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlMultiPacket;
import com.aliyun.gts.sniffer.mypcap.MysqlPacketHandlerDeleted;
import jpcap.packet.TCPPacket;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaoke on 17/11/3.
 */
public class LargePacketMergeThread extends Thread{
    private volatile boolean isRunning=true;
    //如果large queue 满了,那么直接丢掉该包
    public void largePacketQueueAdd(TCPPacket tcp){
        largePacketQueue.offer(tcp);
    }
    //用来缓存大包,生产者消费者模式,提供给merge线程消费
    private ArrayBlockingQueue<TCPPacket> largePacketQueue=new ArrayBlockingQueue<TCPPacket>(Config.maxLargePacketQueueSize);

    //merge线程中大包缓存最大数量
    private final int maxLargePacketMapSize=65535;
    //merge线程会缓存大包,然后每次新包进来,都会触发merge任务,尝试合并为大包,key设计为tcp.ack_num
    private ConcurrentHashMap<Long,MysqlMultiPacket> largePacketMap=new ConcurrentHashMap<Long, MysqlMultiPacket>(maxLargePacketMapSize);
    //统计包大小
    private AtomicLong largePacketMapSize=new AtomicLong(0);

    SqlConsumerThread sqlConsumerThread;

    public ArrayBlockingQueue<TCPPacket> getLargePacketQueue(){
        return largePacketQueue;
    }

    public ConcurrentHashMap<Long,MysqlMultiPacket> getLargePacketMap(){
        return largePacketMap;
    }


    //大包消费者,等待消费
    public void takeLargePacket() throws InterruptedException{
        TCPPacket tcp=largePacketQueue.take();
        mergeLargePacket(tcp);
    }


    public LargePacketMergeThread(SqlConsumerThread sqlConsumerThread){
        this.sqlConsumerThread=sqlConsumerThread;
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
        sqlConsumerThread.packetQueueAdd(dstPacket);
    }

    public boolean isLargeMapFull(){
        if (largePacketMapSize.intValue()>=maxLargePacketMapSize){
            return true;
        }else{
            return false;
        }
    }
    public void largeIncr(){
        largePacketMapSize.getAndIncrement();
    }

    public void largeDecr(){
        largePacketMapSize.getAndDecrement();
    }

    @Override
    public void run(){
        while (isRunning){
            try{
                //阻塞方法,合并大包
                takeLargePacket();
            }catch (InterruptedException e){
                return;
            }
        }
        isRunning=false;
    }
    public void close(){
        isRunning=false;
    }

}
