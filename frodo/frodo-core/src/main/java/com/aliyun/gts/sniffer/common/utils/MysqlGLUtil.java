package com.aliyun.gts.sniffer.common.utils;

import com.aliyun.gts.sniffer.core.Config;

import java.util.regex.Pattern;

public class MysqlGLUtil {
    private static Pattern insertIgnorePattern=Pattern.compile(Config.replacePattern);
    public static boolean matchReplace(String s){
        return insertIgnorePattern.matcher(s).matches();
    }

    private static Pattern numberPattern=Pattern.compile(Config.numberPattern);
    public static boolean matchNumber(String s){
        return numberPattern.matcher(s).matches();
    }
}
