package com.aliyun.gts.sniffer.core;

import java.util.ArrayList;
import java.util.List;

public class Constraint {
    public static List<String> allFilter=new ArrayList<>();

    static {
        // static add
        allFilter.add("ALL");
        allFilter.add("DQL");
        allFilter.add("DML");
    }

}
