package com.aliyun.gts.sniffer.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aliyun.gts.sniffer.common.utils.DateUtil;
import com.aliyun.gts.sniffer.common.utils.FileUtil;
import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.common.utils.PropertiesUtil;
import com.aliyun.gts.sniffer.mypcap.MysqlPacketHandler;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import com.aliyun.gts.sniffer.thread.*;
import com.aliyun.gts.sniffer.thread.generallog.GLCaptureThread;
import com.aliyun.gts.sniffer.thread.generallog.GLConsumerThread;
import com.aliyun.gts.sniffer.thread.generallog.GLProcesslistThread;
import com.aliyun.gts.sniffer.thread.offlinereplay.JSCaptureThread;
import com.aliyun.gts.sniffer.thread.offlinereplay.JSConsumerThread;
import com.aliyun.gts.sniffer.thread.MonitorThread;
import jpcap.JpcapCaptor;
import jpcap.NetworkInterface;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MysqlSniffer {
    private static Logger logger= LoggerFactory.getLogger(MysqlSniffer.class);
    public final static String WARMER_VERSION="1.1.0";
    private static MysqlPacketHandler mysqlPacketHandler=null;
    private static TimeoutPacketCleanThread cleanThread=null;
    private static AbstractCaptureThread captureThread=null;
    private static NetworkInterface device=null;
    private static HashMap<Integer, SqlConsumerThread> netConsumerThreadMap=null;
    private static HashMap<Integer, GLConsumerThread> glConsumerThreadMap=null;
    private static HashMap<Integer,JSConsumerThread> jsConsumerThreadMap=null;
    private static HashMap<Integer,LargePacketMergeThread> packetMergeThreadMap=null;
    private static ExecutorService consumerPool=null;
    private static JDBCUtils srcJdbcUtils=null;
    private static Date start=new Date();
    private static Date end=new Date();

    private static Object stopCondition=new Object();

    private static long captureThreadRestartInterval=60*1000*10;//每隔多少毫秒重启一次抓包线程

    public static void main(String[] args) throws  Exception {
        try{
            initArgs(args);
        }catch (Exception e){
            logger.error("解析命令行参数失败",e);
            System.exit(1);
        }
        //设置日志打印级别
        org.apache.log4j.Logger.getRootLogger().setLevel(Config.getLogLevel());
        logger.info("java.library.path:"+System.getProperty("java.library.path"));
        logger.info("java.ext.dirs:"+System.getProperty("java.ext.dirs"));
        start=new Date();
        logger.info("start time:"+DateUtil.toChar(start));
        String filePath="./app.properties";
        File file =new File(filePath);
        if(file.exists()){
            PropertiesUtil.loadProperty(filePath);
        }
        //开启消费线程
        consumerPool = Executors.newFixedThreadPool(Config.getSqlThreadCnt());


        if(Config.replayTo.equals("mysql")){
            //检查目标库连接可用性
            try{
                String url="jdbc:mysql://"+ Config.getDstIp()+":"+Config.getDstPort()+"/mysql?allowPublicKeyRetrieval=true";
                JDBCUtils jdbcUtils=new JDBCUtils(url,Config.getDstUsername(),Config.getDstPassword(),"mysql");
                jdbcUtils.close();
            }catch (Exception e){
                logger.error("目标库连接失败",e);
                System.exit(1);
            }
        }

        if(Config.captureMethod.equals("json_file")){
            if(!Config.replayTo.equals("mysql")){
                logger.error("from json file,only replay to mysql");
                System.exit(1);
            }
            replayJSONFile();
        }
        if(Config.captureMethod.equals("general_log")){
            captureGL();
        }
        if(Config.captureMethod.equals("net_capture")) {
            captureNet();
        }
        startMonitorThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                MysqlSniffer.stop();
            }
        });
        Timer closeTimer=new Timer();
        closeTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                MysqlSniffer.stop();
            }
        },Config.getTimeSecond()*1000l);

        //打印状态信息
//        closeTimer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                MysqlSniffer.globalStatus();
//            }
//        },0,5000);


        if(Config.captureMethod.equals("net_capture")){
            //10分钟重启一次captureThread
            closeTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try{
                        MysqlSniffer.restartCaptureThread();
                    }catch (InterruptedException e){
                        MysqlSniffer.stop();
                    }
                }
            },captureThreadRestartInterval,captureThreadRestartInterval);
        }

        synchronized (stopCondition){
            stopCondition.wait();
        }

        closeTimer.cancel();
        if(captureThread!=null){
            captureThread.close();
        }
        if(netConsumerThreadMap!=null){
            for(SqlConsumerThread thread:netConsumerThreadMap.values()){
                thread.close();
            }
        }
        if(jsConsumerThreadMap!=null){
            for(JSConsumerThread thread:jsConsumerThreadMap.values()){
                thread.close();
            }
        }

        if(packetMergeThreadMap!=null){
            for(LargePacketMergeThread thread: packetMergeThreadMap.values()){
                thread.close();
            }
        }

        if(glConsumerThreadMap!=null){
            for(GLConsumerThread thread: glConsumerThreadMap.values()){
                thread.close();
            }
        }
        end=new Date();
        aggregateRT();

        Thread.sleep(100);
        consumerPool.shutdown();

        logger.info("exit time:"+DateUtil.toChar(end));
        System.exit(0);
    }
    public static void globalStatus(){
//        String warnInfo="core packet queue size:"+mysqlPacketHandler.getPacketQueueSize()+"/"+mysqlPacketHandler.getMaxPacketQueueSize()+
//                "   large packet queue size:"+mysqlPacketHandler.getLargePacketMapSize()+"/"+mysqlPacketHandler.getMaxLargePacketMapSize()+
//                "   large packet cache timeout count:"+cleanThread.getLargeTimeoutPacketCnt();
        long total=0;
        for(SqlConsumerThread thread:netConsumerThreadMap.values()){
            total+=thread.getRequests();
        }
        logger.warn("total sql:"+total);
    }

    public static void restartCaptureThread()throws InterruptedException{
        captureThread.close();
        Thread.sleep(100);
        logger.warn("restart capture thread");
        captureThread=new CaptureThread(mysqlPacketHandler,device);
        captureThread.start();
    }

    public static void stop(){
        synchronized (stopCondition){
            stopCondition.notify();
        }
    }

    public static void replayJSONFile() throws Exception {
        //开启消费线程
        ExecutorService consumerPool = Executors.newFixedThreadPool(Config.getSqlThreadCnt());

        jsConsumerThreadMap=new HashMap<>();
        for(int i=0;i<Config.getSqlThreadCnt();i++){
            JSConsumerThread consumerThread=new JSConsumerThread();
            consumerPool.execute(consumerThread);
            jsConsumerThreadMap.put(i,consumerThread);
        }
        JSCaptureThread jsCaptureThread=new JSCaptureThread(jsConsumerThreadMap);
        jsCaptureThread.start();
    }

    public static void startMonitorThread(){
        if(!Config.replayTo.equals("mysql")){
            return;
        }

        List<ConsumerThread> consumerThreadList=new ArrayList<>();
        if(glConsumerThreadMap!=null){
            consumerThreadList.addAll(glConsumerThreadMap.values());
        }
        if(netConsumerThreadMap!=null){
            consumerThreadList.addAll(netConsumerThreadMap.values());
        }
        if(jsConsumerThreadMap!=null){
            consumerThreadList.addAll(jsConsumerThreadMap.values());
        }

        MonitorThread monitorThread=new MonitorThread(consumerThreadList);
        monitorThread.start();
    }

    public static void captureNet() throws Exception {
        //检查源库连接可用性
        try{
            String url="jdbc:mysql://"+ Config.getIp()+":"+Config.getPort()+"/mysql?allowPublicKeyRetrieval=true";
            srcJdbcUtils=new JDBCUtils(url,Config.getUsername(),Config.getPassword(),"mysql");
        }catch (Exception e){
            logger.error("源库连接失败",e);
            System.exit(1);
        }

        NetworkInterface[] devices = JpcapCaptor.getDeviceList();
        for (NetworkInterface n : devices) {
            if(n.name.equals(Config.getDeviceName())){
                device=n;
                break;
            }
        }
        if(device==null){
            logger.error(Config.getDeviceName()+" not found");
            System.exit(1);
        }
        MysqlProcessListMetaThread metaThread=new MysqlProcessListMetaThread(MysqlProcessListMeta.getInstance());
        metaThread.start();
        packetMergeThreadMap=new HashMap<Integer, LargePacketMergeThread>();
        netConsumerThreadMap=new HashMap<Integer, SqlConsumerThread>();
        for(int i=0;i<Config.getSqlThreadCnt();i++){
            SqlConsumerThread sqlConsumerThread=new SqlConsumerThread();
            sqlConsumerThread.setSrcJdbcUtils(srcJdbcUtils);
            consumerPool.execute(sqlConsumerThread);
            netConsumerThreadMap.put(i,sqlConsumerThread);
            LargePacketMergeThread largePacketMergeThread=new LargePacketMergeThread(sqlConsumerThread);
            largePacketMergeThread.start();
            packetMergeThreadMap.put(i,largePacketMergeThread);
        }

        if(Config.initPSInfo){
            srcJdbcUtils.initPSInfoOnce(netConsumerThreadMap);
        }
        mysqlPacketHandler=new MysqlPacketHandler(netConsumerThreadMap,packetMergeThreadMap);
        captureThread=new CaptureThread(mysqlPacketHandler,device);
        captureThread.start();
        cleanThread = new TimeoutPacketCleanThread(packetMergeThreadMap);
        cleanThread.start();

    }

    public static void captureGL() throws Exception {
        //检查源库连接可用性
        try{
            String url="jdbc:mysql://"+ Config.getIp()+":"+Config.getPort()+"/mysql?allowPublicKeyRetrieval=true";
            srcJdbcUtils=new JDBCUtils(url,Config.getUsername(),Config.getPassword(),"mysql");
        }catch (Exception e){
            logger.error("源库连接失败",e);
            System.exit(1);
        }
        GLProcesslistThread glProcesslistThread=new GLProcesslistThread(MysqlProcessListMeta.getInstance());
        if(!Config.glCaptureSafe){
            glProcesslistThread.start();
        }else{
            glProcesslistThread.runOnce();
        }
        glConsumerThreadMap=new HashMap<>();
        for(int i=0;i<Config.getSqlThreadCnt();i++){
            GLConsumerThread consumerThread=new GLConsumerThread();
            consumerThread.setSrcJdbcUtils(srcJdbcUtils);
            consumerPool.execute(consumerThread);
            glConsumerThreadMap.put(i,consumerThread);
        }
        GLCaptureThread glCaptureThread=new GLCaptureThread(srcJdbcUtils,glConsumerThreadMap);
        glCaptureThread.start();

    }

    private static void help(String msg){
        if(msg==null){
            System.out.println("mysql data capture and replay tool,version:"+WARMER_VERSION);
            System.out.println("--capture-method general_log or net_capture or json_file");
            System.out.println("--replay-to file:print sql to json file;mysql:execute sql on other mysql instance;stdout:print sql to output");
            System.out.println("--help print this usage information");
            System.out.println("--verbose print version");
            System.out.println("--port listening port");
            System.out.println("--network-device listening network device");
            System.out.println("--time last time(second)");
            System.out.println("--concurrency sql apply thread concurrency");
            System.out.println("--username listening mysql username");
            System.out.println("--password listening mysql password");
            System.out.println("--dst-ip destination mysql ip");
            System.out.println("--dst-port destination mysql port");
            System.out.println("--dst-username destination mysql username");
            System.out.println("--dst-password destination mysql password");
            System.out.println("--log-level log level(info,warn,error)");
            System.out.println("--enlarge enlarge magnification");
            System.out.println("--json-file json sql file path");
            System.out.println("--rate-factor replay rate[0-1],eg:0.1");
        }else{
            System.out.println(msg);
        }
    }

    private static void initArgs(String args[])throws Exception{
        CommandLineParser parser = new GnuParser();
        Options options = new Options();

        options.addOption(null,"capture-method",true,"general_log or net_capture or json_file");
        options.addOption(null,"replay-to",true,"file:print sql to json file;mysql:execute sql on other mysql instance;stdout:print sql to output");
        options.addOption("h", "help", false, "print this usage information");
        options.addOption("v", "verbose", false, "print version" );
        options.addOption("P", "port", true, "source port" );
        options.addOption("u", "username", true, "source username" );
        options.addOption("p", "password", true, "source password" );
        options.addOption("n", "network-device", true, "listening network device" );
        options.addOption("t", "time", true, "warm time second" );
        options.addOption("c", "concurrency", true, "replay thread count" );
        options.addOption("l", "log-level", true, "log level(info warn error)" );
        options.addOption("e", "enlarge", true, "enlarge magnification(integer)" );
        options.addOption(null, "json-file", true, "json sql path" );
        options.addOption(null, "rate-factor", true, "replay rate[0-1],eg:0.1" );
        options.addOption(null, "circle", false, "circle replay,for offline pressure test" );
        options.addOption(null, "commit", false, "commit or rollback,default rollback" );
        options.addOption(null, "sql-timeout", true, "sql timeout ,default 60s" );
        options.addOption(null, "safe", false, "safe mode, only fresh processlist by general log connect and initdb event; else start refresh processlist per 3 second thread" );
        options.addOption(null, "filter", true, "filter sql,[ALL,DQL,DML]: DQL, only select;DML,insert delete update replace; default ALL;" );

        options.addOption("a", "dst-ip", true, "dst ip" );
        options.addOption("b", "dst-port", true, "dst port" );
        options.addOption("x", "dst-username", true, "dst username" );
        options.addOption("d", "dst-password", true, "dst password" );

        // Parse the program arguments
        CommandLine commandLine = parser.parse( options, args );
        if( commandLine.hasOption('h') ) {
            help(null);
            System.exit(0);
        }
        if( commandLine.hasOption('v') ) {
            help(WARMER_VERSION);
            System.exit(0);
        }
        if(commandLine.hasOption("capture-method")){
            String captureMethod=commandLine.getOptionValue("capture-method");
            if(captureMethod.equals("general_log")||captureMethod.equals("net_capture")||captureMethod.equals("json_file")){
                Config.captureMethod=captureMethod;
            }else{
                help(null);
                System.exit(1);
            }
        }else{
            help(null);
            System.exit(1);
        }
        if(commandLine.hasOption("filter")){
            Config.filter.clear();
            String s=commandLine.getOptionValue("filter");
            String[] sArr=s.split(",");
            for(int i=0;i<sArr.length;i++){
                if(!Constraint.allFilter.contains(sArr[i].toUpperCase(Locale.ROOT))){
                    help("not valid args value:"+sArr[i]);
                    System.exit(1);
                }
                Config.filter.add(sArr[i].toUpperCase(Locale.ROOT));
            }
        }


        if(commandLine.hasOption("replay-to")){
            String replayTo=commandLine.getOptionValue("replay-to");
            if(replayTo.equals("stdout")||replayTo.equals("mysql")||replayTo.equals("file")){
                Config.replayTo=replayTo;
            }else{
                help(null);
                System.exit(1);
            }
        }else{
            help(null);
            System.exit(1);
        }

        if(commandLine.hasOption("port")){
            Config.setPort(Integer.valueOf(commandLine.getOptionValue("port")));
            logger.info("source port:"+Config.getPort());
        }
        if(commandLine.hasOption("sql-timeout")){
            Config.sqlTimeout=Integer.valueOf(commandLine.getOptionValue("sql-timeout"));
            logger.info("sql timeout:"+Config.sqlTimeout);
        }

        if(commandLine.hasOption("enlarge")){
            Config.setEnlarge(Integer.valueOf(commandLine.getOptionValue("enlarge")));
            logger.info("sql enlarge magnification:"+Config.getEnlarge());
        }

        if(commandLine.hasOption("username")){
            Config.setUsername(commandLine.getOptionValue("username"));
            logger.info("source username:"+Config.getUsername());
        }

        if(commandLine.hasOption("password")){
            Config.setPassword(commandLine.getOptionValue("password"));
        }

        if(commandLine.hasOption("circle")){
            Config.circle=true;
        }

        if(commandLine.hasOption("safe")){
            Config.glCaptureSafe=true;
        }

        if(commandLine.hasOption("commit")){
            Config.commit=true;
        }

        if(Config.replayTo.equals("mysql")){
            if(commandLine.hasOption("dst-ip")){
                Config.setDstIp(commandLine.getOptionValue("dst-ip"));
                logger.info("dst ip:"+Config.getDstIp());
            }else{
                throw new MissingArgumentException("missing argument --dst-ip");
            }

            if(commandLine.hasOption("dst-port")){
                Config.setDstPort(Integer.valueOf(commandLine.getOptionValue("dst-port")));
                logger.info("dst port:"+Config.getDstPort());
            }else{
                throw new MissingArgumentException("missing argument --dst-port");
            }

            if(commandLine.hasOption("dst-username")){
                Config.setDstUsername(commandLine.getOptionValue("dst-username"));
                logger.info("dst username:"+Config.getDstUsername());
            }else{
                throw new MissingArgumentException("missing argument --dst-username");
            }

            if(commandLine.hasOption("dst-password")){
                Config.setDstPassword(commandLine.getOptionValue("dst-password"));
            }else{
                throw new MissingArgumentException("missing argument --dst-password");
            }
        }

        if(Config.captureMethod.equals("net_capture")){
            if(commandLine.hasOption("network-device")){
                Config.setDeviceName(commandLine.getOptionValue("network-device"));
                logger.info("network device:"+Config.getDeviceName());
            }else{
                throw new MissingArgumentException("missing argument --network-device");
            }
        }

        if(Config.captureMethod.equals("json_file")){
            if(commandLine.hasOption("json-file")){
                Config.replayJSONFilePath=commandLine.getOptionValue("json-file");
                logger.info("network device:"+Config.replayJSONFilePath);
            }else{
                throw new MissingArgumentException("missing argument --json-file");
            }
        }

        if(commandLine.hasOption("time")){
            Config.setTimeSecond(Long.valueOf(commandLine.getOptionValue("time")));
            logger.info("time:"+Config.getTimeSecond()+"s");
        }
        if(commandLine.hasOption("rate-factor")){
            Config.rateFactor=Float.valueOf(commandLine.getOptionValue("rate-factor"));
            logger.info("rate-factor:"+Config.rateFactor);
        }

        if(commandLine.hasOption("concurrency")){
            Config.setSqlThreadCnt(Integer.valueOf(commandLine.getOptionValue("concurrency")));
            logger.info("concurrency:"+Config.sqlThreadCnt);
        }

        if(commandLine.hasOption("log-level")){
            String levelStr=commandLine.getOptionValue("log-level").trim().toLowerCase();
            logger.info("log level:"+levelStr);
            Level logLevel;
            if(levelStr.equals("info")){
                logLevel=Level.INFO;
            }else if(levelStr.equals("warn")){
                logLevel=Level.WARN;
            }else if(levelStr.equals("error")){
                logLevel=Level.ERROR;
            }else if(levelStr.equals("debug")){
                logLevel=Level.DEBUG;
            }else{
                throw new IllegalArgumentException("unknown log level :"+levelStr);
            }
            Config.setLogLevel(logLevel);

        }

    }

    //统计执行耗时结果
    private static void aggregateRT(){
        if(Config.replayTo.equals("mysql")){
            List<ConsumerThread> consumerThreadList=new ArrayList<>();
            if(glConsumerThreadMap!=null){
                consumerThreadList.addAll(glConsumerThreadMap.values());
            }
            if(netConsumerThreadMap!=null){
                consumerThreadList.addAll(netConsumerThreadMap.values());
            }
            if(jsConsumerThreadMap!=null){
                consumerThreadList.addAll(jsConsumerThreadMap.values());
            }
            aggregateRT(consumerThreadList);
        }
    }
    private static void aggregateRT(Collection<ConsumerThread> threads){
        HashMap<String,String> sqlList=new HashMap<String, String>();
        HashMap<String,Long> curMaxExecTime=new HashMap<>();
        HashMap<String,Long> curSumExecTime=new HashMap<>();
        HashMap<String,Long> curMinExecTime=new HashMap<>();
        HashMap<String,Long> curReqSuccessCount=new HashMap<>();
        HashMap<String,Long> curReqCount=new HashMap<>();
        HashMap<String,Long> curReqErrorCount=new HashMap<>();
        for (ConsumerThread thread:threads){
            sqlList.putAll(thread.getSqlList());
            for(Map.Entry<String,Long> entry:thread.getSqlReplayRTNum().entrySet()){
                Long tmpReqSuccessCount=curReqSuccessCount.get(entry.getKey());

                if(tmpReqSuccessCount==null){
                    tmpReqSuccessCount=entry.getValue();
                }else{
                    tmpReqSuccessCount+=entry.getValue();
                }
                curReqSuccessCount.put(entry.getKey(),tmpReqSuccessCount);

                Long tmpReqCount=curReqCount.get(entry.getKey());
                if(tmpReqCount==null){
                    tmpReqCount=entry.getValue();
                }else{
                    tmpReqCount+=entry.getValue();
                }
                curReqCount.put(entry.getKey(),tmpReqCount);
            }
            for(Map.Entry<String,Long> entry:thread.getSqlReplayErrorSum().entrySet()){
                Long tmpExecErrorCount=curReqErrorCount.get(entry.getKey());
                if(tmpExecErrorCount==null){
                    tmpExecErrorCount=entry.getValue();
                }else{
                    tmpExecErrorCount+=entry.getValue();
                }
                curReqErrorCount.put(entry.getKey(),tmpExecErrorCount);

                Long tmpReqCount=curReqCount.get(entry.getKey());
                if(tmpReqCount==null){
                    tmpReqCount=entry.getValue();
                }else{
                    tmpReqCount+=entry.getValue();
                }
                curReqCount.put(entry.getKey(),tmpReqCount);
            }
            for(Map.Entry<String,Long> entry:thread.getSqlReplayRTSum().entrySet()){
                Long tmpSumExecTime=curSumExecTime.get(entry.getKey());
                if(tmpSumExecTime==null){
                    tmpSumExecTime=entry.getValue();
                }else{
                    tmpSumExecTime+=entry.getValue();
                }
                curSumExecTime.put(entry.getKey(),tmpSumExecTime);
            }

            for(Map.Entry<String,Long> entry:thread.getSqlReplayRTMax().entrySet()){
                Long tmpMaxExecTime=curMaxExecTime.get(entry.getKey());
                if(tmpMaxExecTime==null){
                    tmpMaxExecTime=entry.getValue();
                }else{
                    if(tmpMaxExecTime<entry.getValue())
                        tmpMaxExecTime=entry.getValue();
                }
                curMaxExecTime.put(entry.getKey(),tmpMaxExecTime);
            }
            for(Map.Entry<String,Long> entry:thread.getSqlReplayRTMin().entrySet()){
                Long tmpMinExecTime=curMinExecTime.get(entry.getKey());
                if(tmpMinExecTime==null){
                    tmpMinExecTime=entry.getValue();
                }else{
                    if(tmpMinExecTime>entry.getValue())
                        tmpMinExecTime=entry.getValue();
                }
                curMinExecTime.put(entry.getKey(),tmpMinExecTime);
            }

        }
        Double execTime=(end.getTime()-start.getTime())/1000.0;

        String defaultFilePath="./logs/result.csv";
        String header=String.format("sqlId,requests,errors,request/s,success_request/s,error/s,avgRT(ms)/s,minRT(ms)/s,maxRT(ms)/s,sql text");
        BufferedWriter writer = null;
        try{
            writer = new BufferedWriter(new FileWriter(FileUtil.create(defaultFilePath), false));
            writer.write(header);
            writer.write("\n");
            long x=0l;
            for(String s:curReqCount.keySet()){
                String line=String.format("%s,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s",
                        s,
                        curReqCount.get(s),
                        curReqErrorCount.get(s)==null?0:curReqErrorCount.get(s),
                        curReqCount.get(s)/(execTime),
                        (curReqSuccessCount.get(s)==null?0:curReqSuccessCount.get(s))/(execTime),
                        (curReqErrorCount.get(s)==null?0:curReqErrorCount.get(s))/(execTime),
                        (curSumExecTime.get(s)==null?0:curSumExecTime.get(s))/(curReqSuccessCount.get(s)==null?1:curReqSuccessCount.get(s))/1000.0,
                        (curMinExecTime.get(s)==null?0:curMinExecTime.get(s))/1000.0,
                        (curMaxExecTime.get(s)==null?0:curMaxExecTime.get(s))/1000.0,
                        sqlList.get(s).replace("\n"," ").replace("\r"," ")
                );
                if (writer != null){
                    writer.write(line+"\n");
                    if (x%100 == 0){
                        writer.flush();
                    }
                }
                x += 1;
            }
            if (writer != null){
                writer.flush();
                writer.close();
                logger.info("Task end, please check result file:./logs/result.csv");
            }

        }catch (IOException e){
            logger.error("write result csv file failed",e);
        }

    }
}