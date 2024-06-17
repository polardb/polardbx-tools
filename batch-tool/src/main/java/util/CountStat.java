package util;

import java.util.concurrent.atomic.AtomicLong;

public class CountStat {

    /**
     * 数据库读取/写入的行数
     * 非真实的 affected rows 行数
     */
    private final static AtomicLong dbRowCount = new AtomicLong(0);

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