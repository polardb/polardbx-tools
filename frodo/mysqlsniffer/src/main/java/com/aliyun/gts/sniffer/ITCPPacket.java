package com.aliyun.gts.sniffer;

/**
 * Created by zhaoke on 18/6/29.
 */
public interface ITCPPacket {
    public int getDstPort();
    public String getSrcIpV4Address();
    public String getDstIpV4Address();
    public int getSrcPort();
    public byte[] getData();
    public long getAckNum();
    public boolean getPsh();

}
