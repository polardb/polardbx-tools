package util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CountStat {

    /**
     * 数据库读取/写入的行数
     * 非真实的 affected rows 行数
     * 针对单张表的统计
     */
    private final static AtomicLong dbRowCount = new AtomicLong(0);
    /**
     * 理论上不需要 Concurrent
     */
    private final static Map<String, AtomicLong> tableRowCountMap = new ConcurrentHashMap<>();

    public static AtomicLong getTableRowCount(String tableName) {
        return tableRowCountMap.computeIfAbsent(tableName, k -> new AtomicLong(0));
    }

    public static AtomicLong getDbRowCount() {
        return dbRowCount;
    }

    public static void clearDbRowCount() {
        dbRowCount.set(0);
    }

    public static void addDbRowCount(long count) {
        dbRowCount.addAndGet(count);
    }
}