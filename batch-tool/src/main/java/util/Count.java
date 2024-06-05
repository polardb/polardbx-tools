package util;

import java.util.concurrent.atomic.AtomicInteger;

public class Count {
    private static AtomicInteger count = new AtomicInteger(0);
    public static AtomicInteger getCount() {
        return count;
    }
    public static void setCount(AtomicInteger count) {
        Count.count = count;
    }
}