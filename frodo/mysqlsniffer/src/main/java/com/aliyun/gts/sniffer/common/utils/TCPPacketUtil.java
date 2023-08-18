package com.aliyun.gts.sniffer.common.utils;

import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.thread.SqlConsumerThreadDeleted;
import jpcap.packet.TCPPacket;

import java.util.HashMap;

public class TCPPacketUtil {
    public static Integer getThreadKey(TCPPacket packet){
        String keyStr=packet.src_ip.getHostAddress()+":"+packet.src_port;
        return keyStr.hashCode()% Config.getSqlThreadCnt();
    }

    public static Integer getThreadKey(String ip,int port){
        String keyStr=ip+":"+port;
        return getThreadKey(keyStr);
    }

    public static Integer getThreadKey(String ipport){
        return Math.abs(ipport.hashCode())% Config.getSqlThreadCnt();
    }

    public static String getPrepareStmtResponseKey(String ip,int port,int statementId){
        return ip+":"+port+":"+statementId;
    }
    public static String getPrepareStmtResponseKey(String ipport,int statementId){
        return ipport+":"+statementId;
    }


    public static void addTCPPacket(TCPPacket packet, HashMap<Integer, SqlConsumerThreadDeleted> consumerThreadList){
        Integer key=getThreadKey(packet);
        SqlConsumerThreadDeleted t=consumerThreadList.get(key);
        t.packetQueueAdd(packet);
    }
}
