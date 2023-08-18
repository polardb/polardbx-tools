package com.aliyun.gts.sniffer.core;

import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import sun.misc.Signal;
import sun.misc.SignalHandler;


import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.DefaultReport;
import com.aliyun.gts.sniffer.common.entity.ReportResult;
import com.aliyun.gts.sniffer.common.utils.*;
import com.aliyun.gts.sniffer.thread.*;
import com.aliyun.gts.sniffer.thread.offlinereplay.JSConsumerThreadV2;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by junliang.zkk on 22/06/01.
 * author:junliang.zkk
 * email:junliang.zkk@alibaba-inc.com
 * 流量回放工具
 */
public class Frodo {
    private static final Logger logger = LoggerFactory.getLogger(Frodo.class);
    public final static String version = "1.1.29";
    //    private static HashMap<Integer, JSCaptureThread> jsCaptureThreadMap=new HashMap<>();
    private static HashMap<Integer, JSConsumerThreadV2> jsConsumerThreadMap = new HashMap<>();
    private static HashMap<Integer, String> fileShardMap = null;
    public static Long firstSqlStartTime = System.currentTimeMillis();
    public static Long procStartTime = System.currentTimeMillis();
    public static long readCnt = 0L;
    private static Date begin = new Date();
    private static Date end = new Date();
    private static final Object stopCondition = new Object();
    public static final Object startCondition = new Object();
    private static MonitorThread monitorThread;
    private static final HashMap<String, String> sqlList = new HashMap<>();
    private static final HashMap<String, Long> curMaxExecTime = new HashMap<>();
    private static final HashMap<String, Long> curSumExecTime = new HashMap<>();
    private static final HashMap<String, Long> curMinExecTime = new HashMap<>();
    private static final HashMap<String, Long> curReqSuccessCount = new HashMap<>();
    private static final HashMap<String, Long> curReqCount = new HashMap<>();
    private static final HashMap<String, Long> curReqErrorCount = new HashMap<>();
    private static final HashMap<String, Set<String>> curReqErrorMsg = new HashMap<>();
    private static final HashMap<String, Set<String>> curReqSchema = new HashMap<>();


    private static final HashMap<String, Long> originMaxExecTime = new HashMap<>();
    private static final HashMap<String, Long> originSumExecTime = new HashMap<>();
    private static final HashMap<String, Long> originMinExecTime = new HashMap<>();
    private static final HashMap<String, Long> originExecCount = new HashMap<>();

    public static void main(String[] args) throws Exception {
        try {
            initArgs(args);
        } catch (Exception e) {
            logger.error("解析命令行参数失败", e);
            System.exit(1);
        }

        //设置日志打印级别
        org.apache.log4j.Logger.getRootLogger().setLevel(Config.logLevel);
        logger.info("java.library.path:" + System.getProperty("java.library.path"));
        logger.info("java.ext.dirs:" + System.getProperty("java.ext.dirs"));
        String filePath = "./app.properties";
        File file = new File(filePath);
        if (file.exists()) {
            PropertiesUtil.loadProperty(filePath);
        }
        mkdir();
        //如果开启仿真模拟，那么
        if (Config.mode.equals("simulate")) {
            logger.info("mode:simulate,shard by session id,preparing...");
        } else {
            logger.info("mode:random,shard by random,preparing...");
        }
        //探测网络来回耗时
//        try{
//            Config.dstNetworkRoundMicrosecond=getDstNetworkRoundMicrosecond();
//        }catch (Exception e){
//            logger.error("abort,get dst database nework round failed!",e);
//            System.exit(1);
//        }
        fileShardMap = new HashMap<>();
        for (int i = 0; i < Config.sqlThreadCnt; i++) {
            String filePath2 = "./run/" + Config.task + "/.shard" + i;
            fileShardMap.put(i, filePath2);
        }
        if (!Config.skipShard || needShardAndFilter()) {
            //进行SQL分片
            shardAndFilter();
            logger.info("shard and filter done!");
        } else {
            readCnt = count();
            logger.info("skip shard and filter!");
        }
        firstSqlStartTime=getFirstLogTime();
        if(firstSqlStartTime==null){
            logger.error("can not get first log time");
            System.exit(1);
        }
//        replayJSONFile();
        replayJSONFileV2();

        //等待所有线程ready
        while (true) {
            int flag = 0;
            for (ConsumerThread thread : jsConsumerThreadMap.values()) {
                if (thread.isReady()) {
                    flag++;
                }
            }
            if (flag == jsConsumerThreadMap.size()) {
                break;
            }
            Thread.sleep(100);
        }
        //控制线程同时启动
        synchronized (Frodo.startCondition) {
            Frodo.startCondition.notifyAll();
        }
        begin = new Date();
        logger.info("start time:" + DateUtil.toChar(begin));
        procStartTime=System.currentTimeMillis();
        startMonitorThread();
        Runtime.getRuntime().addShutdownHook(new Thread(Frodo::stop));
        //kill -15 手动关闭，并生成报告
        Signal.handle(new Signal("TERM"), new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                stop();
            }
        });

        Timer closeTimer = new Timer();

        closeTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                Frodo.stop();
            }
        }, Config.timeSecond * 1000L);

        //检查sql是否跑完，如果运行完毕，那么终止任务
        closeTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (isThreadDone()) {
                    Frodo.stop();
                }

            }
        }, 0, 500);


        synchronized (stopCondition) {
            stopCondition.wait();
        }
        closeTimer.cancel();

        for (JSConsumerThreadV2 thread : jsConsumerThreadMap.values()) {
            thread.setRunning(false);
        }

//        for(JSCaptureThread thread: jsCaptureThreadMap.values()){
//            thread.setRunning(false);
//        }
        end = new Date();
        //统计输出结果
        aggregateRT(new ArrayList<>(jsConsumerThreadMap.values()));
        Thread.sleep(100);
        logger.info("exit time:" + DateUtil.toChar(end));
        monitorThread.close();
        monitorThread.join();
        System.exit(0);
    }

    public static boolean isThreadDone() {

        for (JSConsumerThreadV2 thread : jsConsumerThreadMap.values()) {
            if (!thread.isClosed()) {
                return false;
            }
        }

//        for(JSCaptureThread thread: jsCaptureThreadMap.values()){
//            if(!thread.isClosed()){
//                return false;
//            }
//        }
        return true;
    }

    public static void stop() {
        synchronized (stopCondition) {
            stopCondition.notify();
        }
    }

    public static void mkdir() {
        File file = new File("./run");
        if (!file.exists()) {
            file.mkdir();
        }
        file = new File("./run/" + Config.task);
        if (file.exists()) {
            file.delete();
        }
        file.mkdir();
    }

    //判断是否需要强制切分日志
    public static boolean needShardAndFilter() {
        File folder = new File("./run/" + Config.task + "/");
        int total = 0;
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.getName().contains(".shard")) {
                total++;
            }
        }
        return total != Config.sqlThreadCnt;
    }

    //当不重新分片的时候，计算总的行数，用于准确打印回放进度。
    public static long count() throws Exception {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec("wc -l " + Config.replayJSONFilePath);
        InputStream inStream = process.getInputStream();
        InputStreamReader inReader = new InputStreamReader(inStream);
        BufferedReader inBuffer = new BufferedReader(inReader);
        process.waitFor();
        String s;
        s = inBuffer.readLine();
        String[] arr = s.split("\\s+");
        return Long.parseLong(arr[0]);
    }

    //拆分文件，用于分片读取sql
    public static void shardAndFilter() throws IOException {
        //清理所有隐藏的.shard文件
        File folder = new File("./run/" + Config.task + "/");
        File[] files = folder.listFiles();
        assert files != null;
        for (File file : files) {
            if (file.getName().contains(".shard")) {
                file.delete();
            }
        }
        HashMap<Integer, BufferedWriter> shardWriter = new HashMap<>();
        //根据新的线程数进行切分
        for (int i = 0; i < Config.sqlThreadCnt; i++) {
            String filePath = "./run/" + Config.task + "/.shard" + i;
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath, false));
            shardWriter.put(i, bufferedWriter);
        }
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(Config.replayJSONFilePath), 20 * 1024 * 1024);

        HashMap<String, Integer> sessionMap = new HashMap<>();
        int sessionMapId = 0;
        String line;
        System.out.println("==============");
        while (true) {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            if (StringUtils.isEmpty(line)) {
                continue;
            }
            JSONObject sqlObject=null;
            try{
                sqlObject = JSONObject.parseObject(line);
            }catch (Exception e){
                logger.error(line);
                throw e;
            }

            String s = sqlObject.getString("session");
            String schema = sqlObject.getString("schema");
            if (Config.schemaMap.size() != 0) {
                if (Config.schemaMap.containsKey(schema)) {
                    sqlObject.put("schema", Config.schemaMap.get(schema));
                } else {
                    continue;
                }
            }
            int key;
            if (Config.mode.equals("simulate")) {
                if (sessionMap.containsKey(s)) {
                    key = sessionMap.get(s);
                } else {
                    key = sessionMapId;
                    sessionMapId++;
                    sessionMap.put(s, key);
                }
            } else {
                key = (int) (readCnt % Config.sqlThreadCnt);
            }
            BufferedWriter writer = shardWriter.get(key % Config.sqlThreadCnt);
            writer.write(sqlObject.toJSONString());
            writer.write("\n");
            readCnt++;
            if (readCnt % 100000 == 0) {
                for (BufferedWriter w : shardWriter.values()) {
                    w.flush();
                }
            }
        }
        for (BufferedWriter w : shardWriter.values()) {
            w.flush();
            w.close();
        }
    }
    public static Long getFirstLogTime() throws IOException{
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(Config.replayJSONFilePath));
        String line=null;
        while(true){
            line = reader.readLine();
            if (line == null) {
                return null;
            }
            if(StringUtils.isEmpty(line)) {
                continue;
            }
            break;
        }
        JSONObject sqlObject = JSONObject.parseObject(line);
        reader.close();
        return sqlObject.getLong("startTime") / 1000;

    }


//    public static void replayJSONFile() throws Exception {
//        jsConsumerThreadMap=new HashMap<>();
//        long timeDiff=System.currentTimeMillis()- BigDecimal.valueOf(firstSqlStartTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue();
//        for(int i=0;i<Config.sqlThreadCnt;i++){
//            JSConsumerThread consumerThread=new JSConsumerThread();
//            consumerThread.setExecTimeDiff(timeDiff);
//            consumerThread.setName("consumer"+i);
//            consumerThread.start();
//            jsConsumerThreadMap.put(i,consumerThread);
//        }
//        jsCaptureThreadMap=new HashMap<>();
//        for(int i=0;i<Config.sqlThreadCnt;i++){
//            HashMap<Integer,JSConsumerThread> tmpMap =new HashMap<>();
//            tmpMap.put(0,jsConsumerThreadMap.get(i));
//            JSCaptureThread jsCaptureThread=new JSCaptureThread(fileShardMap.get(i),tmpMap);
//            jsCaptureThread.setShardId(i);
//            jsCaptureThread.setName("capture"+i);
//            jsCaptureThread.start();
//            jsCaptureThreadMap.put(i,jsCaptureThread);
//        }
//    }

    //探测目标库网络耗时
    public static Long getDstNetworkRoundMicrosecond() throws SQLException {
        JDBCWrapper jdbcWrapper = null;
        if (Config.replayTo.equals("mysql")) {
            String url = "jdbc:mysql://" + Config.host + ":" + Config.port;
            jdbcWrapper = new MysqlWrapper(url, Config.username, Config.password, Config.database);
        } else if (Config.replayTo.equals("polarx")) {
            String url = "jdbc:mysql://" + Config.host + ":" + Config.port;
            jdbcWrapper = new MysqlWrapper(url, Config.username, Config.password, Config.database);
        } else {
            String url = "jdbc:polardb://" + Config.host + ":" + Config.port + "/" + Config.database;
            jdbcWrapper = new PolarOWrapper(url, Config.username, Config.password, Config.database);
        }
        Long dstNetworkRoundMicrosecond = jdbcWrapper.getNetworkRoundMicrosecond();
        jdbcWrapper.close();
        return dstNetworkRoundMicrosecond;
    }

    public static void replayJSONFileV2() throws Exception {
        jsConsumerThreadMap = new HashMap<>();
        long timeDiff = BigDecimal.valueOf(System.currentTimeMillis() - firstSqlStartTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue();
        for (int i = 0; i < Config.sqlThreadCnt; i++) {
            JSConsumerThreadV2 consumerThread = new JSConsumerThreadV2(fileShardMap.get(i));
            consumerThread.setExecTimeDiff(timeDiff);
            consumerThread.setName("consumer" + i);
            consumerThread.start();
            jsConsumerThreadMap.put(i, consumerThread);
        }
    }

    public static void startMonitorThread() {

        List<ConsumerThread> consumerThreadList = new ArrayList<>();

        if (jsConsumerThreadMap != null) {
            consumerThreadList.addAll(jsConsumerThreadMap.values());
        }
        monitorThread = new MonitorThread(consumerThreadList);
        monitorThread.start();
    }

    private static void help(String msg) {
        if (msg == null) {
            System.out.println("db replay tool,version:" + version);
            System.out.println("--source-db oracle|db2|mysql");
            System.out.println("--replay-to mysql|polardb_o|polarx");
            System.out.println("--task self defined task name");
            System.out.println("--mode  simulate|random，simulate：hash by session id ;random hash by random");
            System.out.println("--help print this usage information");
            System.out.println("--verbose print version");
            System.out.println("--schema-map schema map,filter schema; eg:schema1,schema2,schema3:schema1 only replay schema1,schema2,schema3,and schema3 replay to schema1");
            System.out.println("--sql-timeout sql timeout(second) ,default 60s");
            System.out.println("--port listening port");
            System.out.println("--time last time(second)");
            System.out.println("--concurrency sql apply thread concurrency");
            System.out.println("--username username");
            System.out.println("--password password");
            System.out.println("--database database name ");
            System.out.println("--log-level log level(info,warn,error)");
            System.out.println("--enlarge enlarge magnification");
            System.out.println("--file json file path,generate by sql gather tools,eg:mysqlsniffer、sqla");
            System.out.println("--interval report interval(second)");
            System.out.println("--rate-factor replay rate[0-1],eg:0.1");
            System.out.println("--enable-transfer enable sql auto transfer");
            System.out.println("--filter sql,[ALL,DQL,DML]: DQL, only select;DML,insert delete update replace; default ALL;");
            System.out.println("--disable-insert-to-replace disable transfer insert into to replace into ,only for mysql or polarx");
//            System.out.println("--license-path set license file path,default ./license.key");
//            System.out.println("--exclude-network-round if set, will substract network round time for return time statistics");
        } else {
            System.out.println(msg);
        }
    }

    private static void initArgs(String[] args) throws Exception {
        CommandLineParser parser = new GnuParser();
        Options options = new Options();

        options.addOption(null, "replay-to", true, "mysql|polarx");
        options.addOption(null, "source-db", true, "mysql|oracle|postgresql");
        options.addOption(null, "help", false, "print this usage information");
        options.addOption(null, "mode", true, "simulate|random，simulate：hash by session id ;random hash by random");
        options.addOption(null, "task", true, "self defined task name");
        options.addOption("v", "verbose", false, "print version");
        options.addOption("t", "time", true, "last time(second)");
        options.addOption("c", "concurrency", true, "replay thread count");
        options.addOption("l", "log-level", true, "log level(info warn error),default info");
        options.addOption("e", "enlarge", true, "enlarge magnification(integer)");
        options.addOption("f", "file", true, "json sql path");
        options.addOption(null, "rate-factor", true, "replay rate[0-1],eg:0.1");
        options.addOption(null, "schema-map", true, "map src schema to dts schema and filter schema ;eg:schema1,schema2,schema3:schema1,only replay schema1、schema2、schema3,and schema3 map to schema1 ");
        options.addOption(null, "circle", false, "circle replay,for offline pressure test");
        options.addOption(null, "commit", false, "commit or rollback,default rollback");
        options.addOption(null, "enable-transfer", false, "enable sql transfer,only suppert oracle to polardb_o");
        options.addOption(null, "sql-timeout", true, "sql timeout ,default 60s");
        options.addOption(null, "filter", true, "filter sql,[ALL,DQL,DML]: DQL, only select;DML,insert delete update replace; default ALL;");
        options.addOption("h", "host", true, "dst ip");
        options.addOption("P", "port", true, "dst port");
        options.addOption("u", "username", true, "dst username");
        options.addOption("p", "password", true, "dst password");
        options.addOption("d", "database", true, "dst database");
        options.addOption("i", "interval", true, "report interval(second)");
        options.addOption(null, "search_path", true, "self defined search_path");
        options.addOption(null, "disable-lower-schema", false, "by default,use lower case schema when replay sql.");
        options.addOption(null, "force-set-schema", false, "by default,force set search_path or use db  before replay sql.");
        options.addOption(null, "skip-dupli-error-sql", false, "by default,replay every sql.");
        options.addOption(null, "sqlite-path", true, "update running info to sqllite");
        options.addOption(null, "disable-insert-to-replace", false, "disable transfer insert into to replace into ,only for mysql or polarx");
//        options.addOption(null,"exclude-network-round", false, "if set, will substract network round time for return time statistics;");
        //options.addOption(null, "license-path", true, "set license file path,default ./license.key");
        options.addOption(null, "rate-evenness", true, "set rate evenness,double [0.1-1],default 1");
        options.addOption(null, "exclude-sql-id", true, "排除哪些模板sql，多个sqlId以逗号分割");
        options.addOption(null, "generate-sql-id", false, "生成模板sql指纹ID");
        options.addOption(null, "skip-shard", false, "跳过日志切分，前提：没有参数变更，且第一次已经完整切分过一次日志");
        options.addOption(null, "enable-stream-read", false, "开启流式读取");
        options.addOption(null, "disable-transaction", false, "开启流式读取");
        options.addOption(null, "exclude-long-query-time", true, "开启流式读取");
        options.addOption(null, "dst-networkround-microsecond", true, "开启流式读取");
        options.addOption(null, "exclude-network-round", false, "是否排除网络耗时");
        // Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("help")) {
            help(null);
            System.exit(0);
        }
        if (commandLine.hasOption('v')) {
            help(version);
            System.exit(0);
        }
        if (commandLine.hasOption("file")) {
            Config.replayJSONFilePath = commandLine.getOptionValue("file");
            logger.info("json file:" + Config.replayJSONFilePath);
        } else {
            throw new MissingArgumentException("missing argument file");
        }
        //生成sqlid
        if(commandLine.hasOption("generate-sql-id")){
            //todo：生成sqlid
            String sql=readSql(Config.replayJSONFilePath);
            String sqlId=Util.toPolarXSqlId(sql);
            logger.info("sql:"+sql);
            logger.info("sql id:"+sqlId);
            System.exit(0);
        }

        if (commandLine.hasOption("filter")) {
            Config.filter.clear();
            String s = commandLine.getOptionValue("filter");
            String[] sArr = s.split(",");
            for (int i = 0; i < sArr.length; i++) {
                if (!Constraint.allFilter.contains(sArr[i].toUpperCase(Locale.ROOT))) {
                    help("not valid args value:" + sArr[i]);
                    System.exit(1);
                }
                Config.filter.add(sArr[i].toUpperCase(Locale.ROOT));
            }
        }
        if (commandLine.hasOption("mode")) {
            Config.mode = commandLine.getOptionValue("mode");
            logger.info("mode:" + Config.mode);
        }

        if (commandLine.hasOption("exclude-sql-id")) {
            String[] sqlIds=commandLine.getOptionValue("exclude-sql-id").split(",");
            for(String sqlId:sqlIds){
                Config.excludeSqlIdSet.add(sqlId);
            }
            logger.info("exclude-sql-id:" + commandLine.getOptionValue("exclude-sql-id"));
        }
        if (commandLine.hasOption("skip-shard")) {
            Config.skipShard = true;
            logger.info("skipShard:" + Config.skipShard);
        }
        if (commandLine.hasOption("enable-stream-read")) {
            Config.enableStreamRead = true;
            logger.info("enableStreamRead:" + Config.enableStreamRead);
        }
        if (commandLine.hasOption("rate-evenness")) {
            Config.rateEvenness = Double.valueOf(commandLine.getOptionValue("rate-evenness"));
            if (Config.rateEvenness < 0.1 || Config.rateEvenness > 1) {
                help("not valid args value:" + Config.rateEvenness);
                System.exit(1);
            }
        }
        if (commandLine.hasOption("sqlite-path")) {
            Config.sqlLitePath = commandLine.getOptionValue("sqlite-path");
        }
        if (commandLine.hasOption("skip-dupli-error-sql")) {
            Config.skipDupliErrorSql = true;
        }
        if (commandLine.hasOption("schema-map")) {
            String schemaMapStr = commandLine.getOptionValue("schema-map");
            String[] schemaMapArr = schemaMapStr.split(",");
            for (String schemaMapItem : schemaMapArr) {
                String[] schemaMapItemArr = schemaMapItem.split(":");
                if (schemaMapItemArr.length == 2) {
                    Config.schemaMap.put(schemaMapItemArr[0], schemaMapItemArr[1]);
                } else if (schemaMapItemArr.length == 1) {
                    Config.schemaMap.put(schemaMapItemArr[0], schemaMapItemArr[0]);
                } else {
                    help("schema map args parse failed");
                    System.exit(1);
                }
            }
        }
        if (commandLine.hasOption("disable-lower-schema")) {
            Config.diableLowerSchema = true;
        }
        if (commandLine.hasOption("force-set-schema")) {
            Config.forceSetSchema = true;
        }
        if (commandLine.hasOption("disable-insert-to-replace")) {
            Config.disableInsert2Replace = true;
        }

        if (commandLine.hasOption("enable-transfer")) {
            Config.enableSqlTransfer = true;
        }
        if (commandLine.hasOption("search_path")) {
            Config.searchPath = commandLine.getOptionValue("search_path");
            logger.info("search_path:" + Config.searchPath);
        }
        if (commandLine.hasOption("exclude-network-round")) {
            Config.excludeNetworkRound = true;
            logger.info("excludeNetworkRound:" + Config.excludeNetworkRound);
        }
        if (commandLine.hasOption("disable-transaction")) {
            Config.disableTransaction = true;
            logger.info("disableTransaction:" + Config.disableTransaction);
        }
        if (commandLine.hasOption("exclude-long-query-time")) {
            Config.excludeLongQueryTime = (long)(Double.valueOf(commandLine.getOptionValue("exclude-long-query-time"))*1000);
            logger.info("excludeLongQueryTime:" + Config.excludeLongQueryTime);
        }
        if (commandLine.hasOption("dst-networkround-microsecond")) {
            Config.dstNetworkRoundMicrosecond = (long)(Double.valueOf(commandLine.getOptionValue("dst-networkround-microsecond"))*1000);
            logger.info("dstNetworkRoundMicrosecond:" + Config.dstNetworkRoundMicrosecond);
        }

        if (commandLine.hasOption("task")) {
            Config.task = commandLine.getOptionValue("task");
        } else {
            throw new MissingArgumentException("missing argument --task");
        }
        if (commandLine.hasOption("source-db")) {
            Config.sourceDB = commandLine.getOptionValue("source-db").toLowerCase(Locale.ROOT);
        } else {
            throw new MissingArgumentException("missing argument --source-db");
        }

        if (!commandLine.hasOption("replay-to")) {
            throw new MissingArgumentException("missing argument --replay-to");
        }

        String replayTo = commandLine.getOptionValue("replay-to");

        if (replayTo.equals("mysql") || replayTo.equals("polarx")) {
            Config.replayTo = replayTo;
        } else {
            help(null);
            System.exit(1);
        }


        if (commandLine.hasOption("sql-timeout")) {
            Config.sqlTimeout = Integer.parseInt(commandLine.getOptionValue("sql-timeout"));
            logger.info("sql timeout:" + Config.sqlTimeout);
        }

        if (commandLine.hasOption("enlarge")) {
            Config.enlarge = Integer.parseInt(commandLine.getOptionValue("enlarge"));
            logger.info("sql enlarge magnification:" + Config.enlarge);
        }
        if (commandLine.hasOption("interval")) {
            Config.interval = Integer.parseInt(commandLine.getOptionValue("interval"));
            logger.info("monitor interval:" + Config.interval);
        }

        if (commandLine.hasOption("circle")) {
            Config.circle = true;
            logger.info("circle:" + Config.circle);
        }

        if (commandLine.hasOption("commit")) {
            Config.commit = true;
            logger.info("commit:" + Config.commit);
        }

        if (commandLine.hasOption("host")) {
            Config.host = commandLine.getOptionValue("host");
            logger.info("host:" + Config.host);
        } else {
            throw new MissingArgumentException("missing argument --host");
        }

        if (commandLine.hasOption("port")) {
            Config.port = Integer.parseInt(commandLine.getOptionValue("port"));
            logger.info("port:" + Config.port);
        } else {
            throw new MissingArgumentException("missing argument --port");
        }

        if (commandLine.hasOption("username")) {
            Config.username = commandLine.getOptionValue("username");
            logger.info("username:" + Config.username);
        } else {
            throw new MissingArgumentException("missing argument --username");
        }

        if (commandLine.hasOption("password")) {
            Config.password = commandLine.getOptionValue("password");
        } else {
            throw new MissingArgumentException("missing argument --password");
        }

        if (commandLine.hasOption("database")) {
            Config.database = commandLine.getOptionValue("database");
        }

        if (Config.replayTo.equals("polardb_o")) {
            if (!commandLine.hasOption("database")) {
                throw new MissingArgumentException("missing argument --database");
            }
        }


        if (commandLine.hasOption("time")) {
            Config.timeSecond = Long.parseLong(commandLine.getOptionValue("time"));
            logger.info("time:" + Config.timeSecond + "s");
        }
        if (commandLine.hasOption("rate-factor")) {
            Config.rateFactor = Float.parseFloat(commandLine.getOptionValue("rate-factor"));
            logger.info("rate-factor:" + Config.rateFactor);
        }

        if (commandLine.hasOption("concurrency")) {
            Config.sqlThreadCnt = Integer.parseInt(commandLine.getOptionValue("concurrency"));
            logger.info("concurrency:" + Config.sqlThreadCnt);
        }

        if (commandLine.hasOption("log-level")) {
            String levelStr = commandLine.getOptionValue("log-level").trim().toLowerCase();
            logger.info("log level:" + levelStr);
            Level logLevel;
            switch (levelStr) {
                case "info":
                    logLevel = Level.INFO;
                    break;
                case "warn":
                    logLevel = Level.WARN;
                    break;
                case "error":
                    logLevel = Level.ERROR;
                    break;
                case "debug":
                    logLevel = Level.DEBUG;
                    break;
                default:
                    throw new IllegalArgumentException("unknown log level :" + levelStr);
            }
            Config.logLevel = logLevel;

        }

    }

    private static String readSql(String sqlFilePath){
        BufferedReader reader=null;
        try {
            reader = new BufferedReader(new FileReader(Config.replayJSONFilePath));
            String sql="";
            String line="";
            while(true){
                line=reader.readLine();
                if(line!=null){
                    sql+=line;
                }else{
                    break;
                }
            }
            return sql;
        }catch (IOException e){
            logger.error("read sql failed,",e);
            return null;
        }finally {
            if(reader!=null){
                try{
                    reader.close();
                }catch (Exception ignored){

                }
            }
        }
    }


    private static void aggregateRT(Collection<ConsumerThread> threads) {
        for (ConsumerThread thread : threads) {
            sqlList.putAll(thread.getSqlList());
            for (Map.Entry<String, Long> entry : thread.getSqlReplayRTNum().entrySet()) {
                Long tmpReqSuccessCount = curReqSuccessCount.get(entry.getKey());

                if (tmpReqSuccessCount == null) {
                    tmpReqSuccessCount = entry.getValue();
                } else {
                    tmpReqSuccessCount += entry.getValue();
                }
                curReqSuccessCount.put(entry.getKey(), tmpReqSuccessCount);

                Long tmpReqCount = curReqCount.get(entry.getKey());
                if (tmpReqCount == null) {
                    tmpReqCount = entry.getValue();
                } else {
                    tmpReqCount += entry.getValue();
                }
                curReqCount.put(entry.getKey(), tmpReqCount);
            }
            for (Map.Entry<String, Long> entry : thread.getSqlReplayErrorSum().entrySet()) {
                Long tmpExecErrorCount = curReqErrorCount.get(entry.getKey());
                if (tmpExecErrorCount == null) {
                    tmpExecErrorCount = entry.getValue();
                } else {
                    tmpExecErrorCount += entry.getValue();
                }
                curReqErrorCount.put(entry.getKey(), tmpExecErrorCount);

                Long tmpReqCount = curReqCount.get(entry.getKey());
                if (tmpReqCount == null) {
                    tmpReqCount = entry.getValue();
                } else {
                    tmpReqCount += entry.getValue();
                }
                curReqCount.put(entry.getKey(), tmpReqCount);
            }
            for (Map.Entry<String, Long> entry : thread.getSqlReplayRTSum().entrySet()) {
                Long tmpSumExecTime = curSumExecTime.get(entry.getKey());
                if (tmpSumExecTime == null) {
                    tmpSumExecTime = entry.getValue();
                } else {
                    tmpSumExecTime += entry.getValue();
                }
                curSumExecTime.put(entry.getKey(), tmpSumExecTime);
            }
            for (Map.Entry<String, Set<String>> entry : thread.getSqlReplaySchema().entrySet()) {
                Set<String> replaySchemaSet = curReqSchema.get(entry.getKey());
                if (replaySchemaSet == null) {
                    curReqSchema.put(entry.getKey(), entry.getValue());
                } else {
                    replaySchemaSet.addAll(entry.getValue());
                }
            }
            for (Map.Entry<String, Set<String>> entry : thread.getSqlReplayErrorMsg().entrySet()) {
                Set<String> errorMsg = curReqErrorMsg.get(entry.getKey());
                if (errorMsg == null) {
                    curReqErrorMsg.put(entry.getKey(), entry.getValue());
                } else {
                    //最多采样5条错误日志
                    if (errorMsg.size() >= Config.maxErrorMsgSize) {
                        continue;
                    }
                    errorMsg.addAll(entry.getValue());
                }
            }
            for (Map.Entry<String, Long> entry : thread.getSqlReplayRTMax().entrySet()) {
                Long tmpMaxExecTime = curMaxExecTime.get(entry.getKey());
                if (tmpMaxExecTime == null) {
                    tmpMaxExecTime = entry.getValue();
                } else {
                    if (tmpMaxExecTime < entry.getValue())
                        tmpMaxExecTime = entry.getValue();
                }
                curMaxExecTime.put(entry.getKey(), tmpMaxExecTime);
            }
            for (Map.Entry<String, Long> entry : thread.getSqlReplayRTMin().entrySet()) {
                Long tmpMinExecTime = curMinExecTime.get(entry.getKey());
                if (tmpMinExecTime == null) {
                    tmpMinExecTime = entry.getValue();
                } else {
                    if (tmpMinExecTime > entry.getValue())
                        tmpMinExecTime = entry.getValue();
                }
                curMinExecTime.put(entry.getKey(), tmpMinExecTime);
            }
            //todo:stat origin exec time
            for (Map.Entry<String, Long> entry : thread.getOriginMinExecTime().entrySet()) {
                Long tmpMinExecTime = originMinExecTime.get(entry.getKey());
                if (tmpMinExecTime == null) {
                    tmpMinExecTime = entry.getValue();
                } else {
                    if (tmpMinExecTime > entry.getValue())
                        tmpMinExecTime = entry.getValue();
                }
                originMinExecTime.put(entry.getKey(), tmpMinExecTime);
            }

            for (Map.Entry<String, Long> entry : thread.getOriginMaxExecTime().entrySet()) {
                Long tmpMinExecTime = originMaxExecTime.get(entry.getKey());
                if (tmpMinExecTime == null) {
                    tmpMinExecTime = entry.getValue();
                } else {
                    if (tmpMinExecTime < entry.getValue())
                        tmpMinExecTime = entry.getValue();
                }
                originMaxExecTime.put(entry.getKey(), tmpMinExecTime);
            }

            for (Map.Entry<String, Long> entry : thread.getOriginExecCount().entrySet()) {
                Long tmpMinExecTime = originExecCount.get(entry.getKey());
                if (tmpMinExecTime == null) {
                    tmpMinExecTime = entry.getValue();
                } else {
                    tmpMinExecTime += entry.getValue();
                }
                originExecCount.put(entry.getKey(), tmpMinExecTime);
            }

            for (Map.Entry<String, Long> entry : thread.getOriginSumExecTime().entrySet()) {
                Long tmpMinExecTime = originSumExecTime.get(entry.getKey());
                if (tmpMinExecTime == null) {
                    tmpMinExecTime = entry.getValue();
                } else {
                    tmpMinExecTime += entry.getValue();
                }
                originSumExecTime.put(entry.getKey(), tmpMinExecTime);
            }
        }
        aggregateAndWrite();
    }

    private static void aggregateAndWrite() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String strDate = formatter.format(begin);
        String resultPath = "./run/" + Config.task + "/" + strDate;
        File result = new File(resultPath);
        if(!result.exists()){
            result.mkdirs();
        }
        String defaultJsonFilePath = resultPath + "/result.json";
        String defaultFilePath = resultPath + "/result.xlsx";
        logger.info("result file:" + defaultFilePath);
        ExcelWriter excelWriter = null;
        BufferedWriter writer;
        try {
            //获取每条SQL的统计信息
            List<DefaultReport> data2 = getDefaultReportList();
            //获取概要统计信息
            List<ReportResult> data = getGeneralReport(data2);
//            //写入json结果文件，遇到过大结果集导致json生成慢的问题，先注释掉
//            writer = new BufferedWriter(new FileWriter(defaultJsonFilePath, false));
//            JSONObject object = new JSONObject();
//            object.put("general", JSON.toJSON(data));
//            object.put("data", JSON.toJSON(data2));
//            writer.write(object.toJSONString());
//            writer.flush();
//            writer.close();

            excelWriter = EasyExcel.write(defaultFilePath).build();
            WriteSheet writeSheet = EasyExcel.writerSheet(0, "概要报告").head(ReportResult.class).build();
            excelWriter.write(data, writeSheet);
            WriteSheet writeSheet2 = EasyExcel.writerSheet(1, "SQL详情").head(DefaultReport.class).build();
            excelWriter.write(data2, writeSheet2);
        } catch (Exception e) {
            logger.error("write result file failed,", e);
        } finally {
            if (excelWriter != null) {
                excelWriter.finish();
            }
        }
    }


    private static List<DefaultReport> getDefaultReportList() {
        long end = System.currentTimeMillis() / 1000;
        double execTime = (double) (end - begin.getTime() / 1000);
        if (execTime == 0) {
            execTime = 1;
        }
        List<DefaultReport> reports = new ArrayList<>();
        for (String s : originExecCount.keySet()) {
            if (curReqCount.get(s) == null) {
                continue;
            }
            DefaultReport report = new DefaultReport();
            report.setSqlId(s);
            report.setRequest(curReqCount.get(s) == null ? 0 : curReqCount.get(s));
            report.setReqPerSecond(report.getRequest() / execTime);
            report.setErrorReq(curReqErrorCount.get(s) == null ? 0 : curReqErrorCount.get(s));
            report.setErrorReqPerSecond((curReqErrorCount.get(s) == null ? 0 : curReqErrorCount.get(s)) / execTime);
            report.setOriginMinRT(originMinExecTime.get(s) * 1.0);
            report.setMinRT((curMinExecTime.get(s) == null ? 0 : curMinExecTime.get(s)) * 1.0);
            report.setOriginAvgRT(originSumExecTime.get(s) / originExecCount.get(s) * 1.0);
            report.setAvgRT(curSumExecTime.get(s) == null ? 0 : curSumExecTime.get(s) * 1.0 / (curReqSuccessCount.get(s) == 0 ? 1 : curReqSuccessCount.get(s)));
            report.setOriginMaxRT((originMaxExecTime.get(s) == null ? 0 : originMaxExecTime.get(s)) * 1.0);
            report.setMaxRT((curMaxExecTime.get(s) == null ? 0 : curMaxExecTime.get(s)) * 1.0);
            report.setSchema(getSchemaStr(curReqSchema.get(s)));
            if (sqlList.get(s).length() > 10000) {
                report.setSampleSql(sqlList.get(s).substring(0, 9997)+"...");
            } else {
                report.setSampleSql(sqlList.get(s));
            }
            report.setErrorMsg(getErrorMsg(curReqErrorMsg.get(s)));

//            if (curReqErrorCount.get(s) != null && curReqErrorCount.get(s) > 0 && Config.enableSqlTransfer) {
//                try {
//                    //对于报错的sql，提供改写建议，只适用于oracle
//                    TranslateReport translateReport = SqlTranslator.translate(sqlList.get(s), config);
//                    if (translateReport.getTranslatedSql().length() > 10000) {
//                        report.setTransferSql(translateReport.getTranslatedSql().substring(0, 10000));
//                    } else {
//                        report.setTransferSql(translateReport.getTranslatedSql());
//                    }
//
//                    List<ChangedReportInfo> changes = translateReport.getChanges();
//                    List<UnsupportedReportInfo> errors = translateReport.getUnsupported();
//                    StringBuilder changeStr = new StringBuilder();
//                    StringBuilder unsupportedStr = new StringBuilder();
//                    for (ChangedReportInfo changedReportInfo : changes) {
//                        int reportId = changedReportInfo.getReportId();
//                        ConvertDescriptionInfo convertDescriptionInfo = ConvertDescriptionHolder.get(reportId, DescriptionLanguage.CN);
//                        changeStr.append(convertDescriptionInfo.getDetail()).append(";\n");
//                    }
//                    for (UnsupportedReportInfo unsupportedReportInfo : errors) {
//                        int reportId = unsupportedReportInfo.getReportId();
//                        ConvertDescriptionInfo convertDescriptionInfo = ConvertDescriptionHolder.get(reportId, DescriptionLanguage.CN);
//                        unsupportedStr.append(convertDescriptionInfo.getDetail()).append(";\n");
//                    }
//                    if (changeStr.length() > 10000) {
//                        report.setTransferItem(changeStr.substring(0, 10000));
//                    } else {
//                        report.setTransferItem(changeStr.toString());
//                    }
//                    if (unsupportedStr.length() > 10000) {
//                        report.setTransferError(unsupportedStr.substring(0, 10000));
//                    } else {
//                        report.setTransferError(unsupportedStr.toString());
//                    }
//                } catch (Exception e) {
//                    logger.error("adam transfer sql failed", e);
//                }
//            }

            reports.add(report);
        }
        return reports;
    }

    private static String getSchemaStr(Set<String> schemaSet) {
        if (schemaSet == null || schemaSet.size() == 0) {
            return "";
        }
        StringBuilder str = new StringBuilder();
        Iterator<String> iterator = schemaSet.iterator();
        while (iterator.hasNext()) {
            String tmp = iterator.next();
            if (!iterator.hasNext()) {
                str.append(tmp);
            } else {
                str.append(tmp);
                str.append(",");
            }
        }
        if (str.length() > 10000) {
            return str.substring(0, 10000);
        } else {
            return str.toString();
        }
    }

    private static String getErrorMsg(Set<String> errorSet) {
        if (errorSet == null || errorSet.size() == 0) {
            return "";
        }
        StringBuilder str = new StringBuilder();
        for (String s : errorSet) {
            str.append(s);
            str.append(";\n");
        }
        if (str.length() > 10000) {
            return str.substring(0, 10000);
        } else {
            return str.toString();
        }
    }

    private static List<ReportResult> getGeneralReport(List<DefaultReport> reportList) {
        ReportResult reportResult = new ReportResult();
        reportResult.setSourceDbType(Config.sourceDB);
        reportResult.setDstDbType(Config.replayTo);
        reportResult.setExecTime((end.getTime() - begin.getTime()) / 1000);
        reportResult.setSqlTemplateCnt(sqlList.size());
        //success requests
        Long tmp = 0L;
        for (Long x : curReqCount.values()) {
            tmp += x;
        }

        reportResult.setReqCnt(tmp);
        //error requests
        tmp = 0L;
        for (Long x : curReqErrorCount.values()) {
            tmp += x;
        }
        reportResult.setErrReqCnt(tmp);

        //计算模板sql 成功率
        tmp = 0L;
        for (String sqlId : sqlList.keySet()) {
            Long errNum = curReqErrorCount.get(sqlId);
            if (errNum != null && errNum != 0) {
                tmp++;
            }
        }
        reportResult.setSqlTemplateErrCnt(tmp);
        reportResult.setSqlTemplateSuccessCnt(sqlList.size() - tmp);
        reportResult.setSqlTemplateCompatibility((sqlList.size() - tmp) * 1.0 / sqlList.size());
        //总的执行时间
        tmp = 0L;
        for (Long x : curSumExecTime.values()) {
            tmp += x;
        }
        reportResult.setSuccessReqCnt(reportResult.getReqCnt() - reportResult.getErrReqCnt());
        reportResult.setAvgReqTime(tmp * 1.0 / (reportResult.getSuccessReqCnt() == 0 ? 1 : reportResult.getSuccessReqCnt()));
        reportResult.setSuccessReqRatio(reportResult.getSuccessReqCnt() * 1.0 / (reportResult.getReqCnt() == 0 ? 1 : reportResult.getReqCnt()));

        long avgReqTimeGT10S = 0L;
        long avgReqTimeGT1S = 0L;
        long avgReqTimeGT100MS = 0L;
        long avgReqTimeGT10MS = 0L;
        long avgReqTimeGT1MS = 0L;
        long avgReqTimeLT1MS = 0L;

        //分段耗时统计
        for (DefaultReport report : reportList) {
            if (report.getErrorReq() > 0) {
                continue;
            }
            if (report.getAvgRT() > 10 * 1000 * 1000) {
                avgReqTimeGT10S++;
                continue;
            }
            if (report.getAvgRT() > 1 * 1000 * 1000) {
                avgReqTimeGT1S++;
                continue;
            }
            if (report.getAvgRT() > 100 * 1000) {
                avgReqTimeGT100MS++;
                continue;
            }
            if (report.getAvgRT() > 10 * 1000) {
                avgReqTimeGT10MS++;
                continue;
            }
            if (report.getAvgRT() > 1 * 1000) {
                avgReqTimeGT1MS++;
                continue;
            }
            avgReqTimeLT1MS++;
        }
        reportResult.setAvgReqTimeGT10S(avgReqTimeGT10S);
        reportResult.setAvgReqTimeGT1S(avgReqTimeGT1S);
        reportResult.setAvgReqTimeGT100MS(avgReqTimeGT100MS);
        reportResult.setAvgReqTimeGT10MS(avgReqTimeGT10MS);
        reportResult.setAvgReqTimeGT1MS(avgReqTimeGT1MS);
        reportResult.setAvgReqTimeLT1MS(avgReqTimeLT1MS);
        List<ReportResult> results = new ArrayList<>();
        results.add(reportResult);
        return results;
    }

}