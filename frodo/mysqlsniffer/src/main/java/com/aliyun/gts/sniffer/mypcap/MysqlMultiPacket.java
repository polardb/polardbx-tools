package com.aliyun.gts.sniffer.mypcap;

import com.aliyun.gts.sniffer.common.utils.NumberUtil;
import com.aliyun.gts.sniffer.mysql.MysqlCmdType;
import jpcap.packet.TCPPacket;

import java.util.Comparator;
import java.util.Date;
import java.util.TreeMap;

/**
 * Created by zhaoke on 17/10/31.
 */
public class MysqlMultiPacket {
    private int maxPacketCnt=100;//大包拆分的最大个数,超过就丢弃,避免因为一些上百兆的包撑爆内存
    TreeMap<Long, TCPPacket> packetTreeMap = new TreeMap<Long, TCPPacket>(new Comparator<Long>(){
        /*
         * int compare(Object o1, Object o2) 返回一个基本类型的整型，
         * 返回负数表示：o1 小于o2，
         * 返回0 表示：o1和o2相等，
         * 返回正数表示：o1大于o2。
         */
        public int compare(Long o1, Long o2) {
            //指定排序器按照升序排列
            return o1.compareTo(o2);
        }
    });
    //用于清理过期包
    private Date updated; //如果packet长时间不更新,那么可以确认这些缓存的包是失效的
    private TCPPacket firstPacket=null;

    public MysqlMultiPacket(){
        updated=new Date();
    }

    public Date getUpdated() {
        return updated;
    }

    public TCPPacket addPacket(TCPPacket packet){
        updated=new Date();
        if(packetTreeMap.size()>maxPacketCnt){
            return null;
        }
        packetTreeMap.put(packet.sequence,packet);
        return generate();
    }

    private TCPPacket generate(){
        if(!complete()){
            return null;
        }
        firstPacket=packetTreeMap.firstEntry().getValue();
        int length= NumberUtil.threeLEByte2Int(firstPacket.data,0);
        //构造
        byte[] dstByte=new byte[length+4];
        int start=0;
        for (TCPPacket tcp:packetTreeMap.values()){
            System.arraycopy(tcp.data,0,dstByte,start,tcp.data.length);
            start+=tcp.data.length;
        }
        firstPacket.data=dstByte;
        return firstPacket;

    }

    private boolean complete(){
        TCPPacket tcp= packetTreeMap.firstEntry().getValue();
        byte[] data=tcp.data;
        int length= NumberUtil.threeLEByte2Int(data,0);
        byte type=data[4];
        int curLength=0;
        for(TCPPacket tmp:packetTreeMap.values()){
            curLength+=tmp.data.length;
        }

        if( length==(curLength-4)&&(type== MysqlCmdType.COM_QUERY ||type==MysqlCmdType.COM_STMT_PREPARE||type==MysqlCmdType.COM_STMT_EXECUTE)){
           return true;
        }
        return false;
    }
}
