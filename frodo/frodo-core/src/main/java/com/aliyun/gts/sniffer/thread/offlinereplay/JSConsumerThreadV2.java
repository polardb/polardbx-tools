package com.aliyun.gts.sniffer.thread.offlinereplay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.gts.sniffer.common.entity.BaseSQLType;
import com.aliyun.gts.sniffer.common.utils.HexUtil;
import com.aliyun.gts.sniffer.common.utils.MysqlWrapper;
import com.aliyun.gts.sniffer.common.utils.PolarOWrapper;
import com.aliyun.gts.sniffer.common.utils.Util;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.core.Frodo;
import com.aliyun.gts.sniffer.thread.ConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;

public class JSConsumerThreadV2 extends ConsumerThread {
    private final Logger logger = LoggerFactory.getLogger(JSConsumerThreadV2.class);
    //计算json文件中的第一条sql的执行时间和当前时间的差值，用于模拟实际速度重放sql
    private long execTimeDiff = 0L;
    private final String file;

    public void setExecTimeDiff(long execTimeDiff) {
        this.execTimeDiff = execTimeDiff;
    }

    public JSConsumerThreadV2(String file) throws SQLException {
        this.file = file;
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
    }

    public void close() {
        running = false;
    }

    @Override
    public void run() {
        logger.info("consumer thread " + Thread.currentThread().getName() + " created!");
        BufferedReader reader;
        try {
            //2MB的缓冲区，提前创建
            reader = new BufferedReader(new FileReader(file), 2 * 1024 * 1024);
            synchronized (Frodo.startCondition) {
                this.ready = true;
                Frodo.startCondition.wait();
            }
        } catch (Exception e) {
            logger.error("consumer thread " + Thread.currentThread().getName() + " start failed!", e);
            return;
        }
        logger.info("consumer thread " + Thread.currentThread().getName() + " start!");
        try {
            String line;
            while (running) {
                line = reader.readLine();
                //如果为null
                if (line == null) {
                    reader.close();
                    if (Config.circle && running) {
                        reader.close();
                        reader = new BufferedReader(new FileReader(file));
                        continue;
                    }
                    break;
                }
                try {
                    applyJSONSql(line);
                } catch (Exception e) {
                    logger.error("apply sql failed", e);
                }
            }
        } catch (Exception e) {
            logger.error("thread exit!!!",e);
        }
        delay = 0L;
        closed = true;
        logger.info("consumer thread " + Thread.currentThread().getName() + " closed!");
        jdbcWrapper.close();
    }

    private void applyJSONSql(String sqlJSON) {
        JSONObject object = JSON.parseObject(sqlJSON);
        if (object == null) {
            return;
        }
        JSONArray parameter=object.getJSONArray("parameter");
        String sql = object.getString("convertSqlText");
        String db = object.getString("schema");
        //默认把schema转成小写。
        if (!Config.diableLowerSchema) {
            db = db.toLowerCase(Locale.ROOT);
        }
        String sqlId = object.getString("sqlId");
        long originExecTime = object.getLong("execTime");
        if (Config.sourceDB.equals("oracle")) {
            sqlId = Util.toOracleSqlId(sql, sqlId);
        }

        if (Config.sourceDB.equals("polarx") || sqlId == null) {
            sqlId = Util.toPolarXSqlId(sql);
        }

        //全小写，方便处理，同时去除select头部的hint，避免hint影响sql语句类型判断
        String sql2 = Util.trimHeaderHint(sql.toLowerCase(Locale.ROOT));
        //过滤掉commit rollback begin start transaction
        if (sql2.startsWith("begin") || sql2.startsWith("commit")
                || sql2.startsWith("rollback") || sql2.startsWith("start")) {
            skipRequestCnt++;
            return;
        }
        //过滤掉人为指定的模板sql
        if(Config.excludeSqlIdSet.contains(sqlId)){
            skipRequestCnt++;
            return;
        }

        //默认过滤掉set reset show等命令
        if (filterDefault(sql2)) {
            skipRequestCnt++;
            return;
        }
        //判断SQL类型
        BaseSQLType sqlType = getSQLType(sql2);
        //是否命中需要输出的sql，如果没命中，那么忽略sql
        if (!filterSQL(sql2, sqlType)) {
            skipRequestCnt++;
            return;
        }
        //单位毫秒
        long startTime = object.getLong("startTime") / 1000;
        //计算预计执行时间和当前执行时间的差值。
//        long a = BigDecimal.valueOf(System.currentTimeMillis() - startTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue();
//        long actTimeDiff = execTimeDiff - a;
        long actTimeDiff=BigDecimal.valueOf(startTime-Frodo.firstSqlStartTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()-
                BigDecimal.valueOf(System.currentTimeMillis()-Frodo.procStartTime).longValue();
        if (actTimeDiff > 0 && !Config.circle && Config.rateFactor != 0.0f) {
            try {
                long sleepMills = actTimeDiff;
                Thread.sleep(sleepMills);
                actTimeDiff=BigDecimal.valueOf(startTime-Frodo.firstSqlStartTime).multiply(BigDecimal.valueOf(Config.rateFactor)).longValue()-
                        BigDecimal.valueOf(System.currentTimeMillis()-Frodo.procStartTime).longValue();
            } catch (InterruptedException ignored) {

            }
        }
        delay = actTimeDiff * -1;
        apply(sql, sql2, db, sqlId, originExecTime, sqlType,parameter);
    }
}
