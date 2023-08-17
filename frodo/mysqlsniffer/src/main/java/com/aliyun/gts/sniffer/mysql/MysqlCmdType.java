package com.aliyun.gts.sniffer.mysql;

/**
 * Created by zhaoke on 17/10/28.
 */
public class MysqlCmdType {
    public static byte COM_QUERY=0x03;
    public static byte COM_QUIT=0x01;
    public static byte COM_INIT_DB=0x02;
    public static byte COM_STMT_PREPARE=0x16;
    public static byte COM_STMT_EXECUTE=0x17;
    public static byte COM_STMT_CLOSE=0x19;
    public static byte COM_STMT_RESET=0x1a;
    public static byte OK_PACKET=0x00;




}
