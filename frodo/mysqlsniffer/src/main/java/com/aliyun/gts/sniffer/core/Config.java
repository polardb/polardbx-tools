package com.aliyun.gts.sniffer.core;

import org.apache.log4j.Level;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaoke on 18/3/30.
 */
public class Config implements Serializable{
    public static int port=3306;//源库端口,抓包监听端口
    public final static String ip="127.0.0.1";//源库ip
    public static String username;//源库用户名
    public static String password;//源库密码
    public static int dstPort=3306;//目标库端口
    public static String dstIp;//目标库ip
    public static String dstUsername;//目标库用户名
    public static String dstPassword;//目标库密码
    public static String deviceName;//网卡名称
    public static long timeSecond=100;
    public static int sqlThreadCnt=4;
    public static Level logLevel= Level.INFO;//设置日志级别
    public static int enlarge =1;//sql 放大倍数,默认和真实流量一样
    public static String replayTo="stdout";
    public static String captureMethod="net_capture"; //net_capture、general_log、json_file
    public static String replayJSONFilePath="";
    public static int interval=5;
    public static float rateFactor=1;
    public static boolean circle=false;//判断是否是循环重放。
    public static boolean commit=false;//是否提交
    public static int sqlTimeout=60;
    public static boolean simulate=false;//是否模拟用户行为，默认sql全部打散到所有线程，如果开区，按照threadId进行hash打散
    //如果为true，那么在glcapture启动之前只做一次processlist的采集，后续依赖general log里的connect 和init db事件维护processlist；
    // 如果是false，那么会启动processlist维护线程，每隔3秒更新一次processlist,如果有大量use db的操作，有可能导致部分sql匹配到错误的DB
    public static boolean glCaptureSafe=false;


    public static boolean filterSelect=true;

    public static int maxPrepareStmtCacheSize=65535;
    public static int maxPacketQueueSize=65535;//单个线程缓存packet数量，
    public static int maxLargePacketQueueSize=65535;//单个线程缓存分包数量
    public static boolean stringToHex=false;//是否开启16进制字符串显示
    public static boolean initPSInfo=false;//是否开启prepare statement 启动时初始化。
    public static int maxProcesslistCacheSize=655350;//processlist 缓存大小，单个线程
    public static int maxGLTailCacheSize=655350;//general log 监听缓存大小，单个线程
    public static int processlistRefreshInterval=3;//processlist更新间隔，单位秒

    public static String glEndPattern="^20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}[A-Z]+\t\\d+\\s+.*";
    public static String glStartPattern="^20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}[A-Z]+\t\\d+\\s+(Execute|Query).*";

    public static String glConnectPattern="^20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}[A-Z]+\t\\d+\\s+(Connect)\\s+.*(on)\\s+.*(using)\\s+.*";
    public static String glInitDBPattern="^20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}[A-Z]+\t\\d+\\s+(Init DB)\\s+.*";

    public static String insertIgnorePattern="^insert\\s+ignore";

    public static Integer maxCaptureOutLength=30000;

    // all dml dql
    public static List<String> filter=new ArrayList<>();
    static {
        filter.add("ALL");
    }


//    private static boolean withDml=false;//是否包含DML语句
//    public static boolean isWithDml() {
//        return withDml;
//    }
//
//    public static void setWithDml(boolean withDml) {
//        Config.withDml = withDml;
//    }



    public static int getEnlarge() {
        return enlarge;
    }

    public static void setEnlarge(int enlarge) {
        Config.enlarge = enlarge;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        Config.port = port;
    }

    public static String getIp() {
        return ip;
    }

    public static String getUsername() {
        return username;
    }

    public static void setUsername(String username) {
        Config.username = username;
    }

    public static String getPassword() {
        return password;
    }

    public static void setPassword(String password) {
        Config.password = password;
    }

    public static int getDstPort() {
        return dstPort;
    }

    public static void setDstPort(int dstPort) {
        Config.dstPort = dstPort;
    }

    public static String getDstIp() {
        return dstIp;
    }

    public static void setDstIp(String dstIp) {
        Config.dstIp = dstIp;
    }

    public static String getDstUsername() {
        return dstUsername;
    }

    public static void setDstUsername(String dstUsername) {
        Config.dstUsername = dstUsername;
    }

    public static String getDstPassword() {
        return dstPassword;
    }

    public static void setDstPassword(String dstPassword) {
        Config.dstPassword = dstPassword;
    }

    public static String getDeviceName() {
        return deviceName;
    }

    public static void setDeviceName(String deviceName) {
        Config.deviceName = deviceName;
    }

    public static long getTimeSecond() {
        return timeSecond;
    }

    public static void setTimeSecond(long timeSecond) {
        Config.timeSecond = timeSecond;
    }

    public static int getSqlThreadCnt() {
        return sqlThreadCnt;
    }

    public static void setSqlThreadCnt(int sqlThreadCnt) {
        Config.sqlThreadCnt = sqlThreadCnt;
    }

    public static Level getLogLevel() {
        return logLevel;
    }

    public static void setLogLevel(Level logLevel) {
        Config.logLevel = logLevel;
    }
}
