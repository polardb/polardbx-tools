package com.aliyun.gts.sniffer.core;

import org.apache.log4j.Level;

import java.io.Serializable;
import java.util.*;


public class Config implements Serializable{
    public static String task="task1";
    public static String sourceDB="oracle";
    public static int port=3306;//源库端口,抓包监听端口
    public static String host="127.0.0.1";//源库ip
    public static String username;//源库用户名
    public static String password;//源库密码
    public static long timeSecond=86400;
    public static int sqlThreadCnt=4;
    public static Level logLevel= Level.INFO;//设置日志级别
    public static int enlarge =1;//sql 放大倍数,默认和真实流量一样
    public static String replayTo="mysql";
    public static String replayJSONFilePath="";
    public static int interval=5;
    public static float rateFactor=1;
    public static boolean circle=false;//判断是否是循环重放。
    public static boolean commit=false;//是否提交
    public static int sqlTimeout=60;
    public static String mode="simulate";//simulate|random 是否模拟用户行为，simulate按照threadId进行hash打散;random 随机打散sql。
    public static String database="mysql";
    public static Map<String,String> schemaMap=new HashMap<>();
    public static boolean enableSqlTransfer=false;
    public static int maxErrorMsgSize=5;
    public static String searchPath=null;
    public static boolean diableLowerSchema=false;
    public static boolean forceSetSchema=false; //默认不开启force set schema
    public static boolean skipDupliErrorSql=false;
    public static String sqlLitePath=null;
    public static long maxConnectionExpiredMS=30*60*1000;//空闲连接超时时间，超过改时间需要检查链接是否断开
    public static boolean disableInsert2Replace=false;
    public static boolean skipShard=false;
    public static long longQueryTime=1000l;
    public static boolean enableStreamRead=false;
    public static HashSet<String> excludeSqlIdSet=new HashSet<String>();
    public static boolean disableTransaction=false;

    public static long excludeLongQueryTime =-1L; //是否排除超过该阈值的SQL，单位微秒
    public static Double rateEvenness=1d;

    public static int maxPacketQueueSize=65535;//单个线程缓存packet数量，
    public static String replacePattern="^replace\\s+into";
    public static boolean excludeNetworkRound=false;//是否排除网络来回时间影响，默认false，那么会探测网络来回耗时。在计算RT时，减去网络来回耗时。
    public static Long dstNetworkRoundMicrosecond= -1L;//设置网络来回的耗时，单位微秒，在统计SQL耗时时，减去网络耗时，单位微秒

    // all dml dql
    public static List<String> filter=new ArrayList<>();
    static {
        filter.add("ALL");
    }
}
