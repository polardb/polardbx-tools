package com.aliyun.gts.sniffer;


public interface ITCPPacket {
    public int getDstPort();
    public String getSrcIpV4Address();
    public String getDstIpV4Address();
    public int getSrcPort();
    public byte[] getData();
    public long getAckNum();
    public boolean getPsh();

}
