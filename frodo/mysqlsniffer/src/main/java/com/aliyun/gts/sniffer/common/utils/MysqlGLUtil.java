package com.aliyun.gts.sniffer.common.utils;

import com.aliyun.gts.sniffer.core.Config;

import java.util.regex.Pattern;

public class MysqlGLUtil {
    private static Pattern endPattern=Pattern.compile(Config.glEndPattern);
    private static Pattern startPattern=Pattern.compile(Config.glStartPattern);
    private static Pattern connPattern=Pattern.compile(Config.glConnectPattern);
    private static Pattern initDBPattern=Pattern.compile(Config.glInitDBPattern);
    private static Pattern insertIgnorePattern=Pattern.compile(Config.insertIgnorePattern);
    private static Pattern useDBPattern=Pattern.compile(Config.useDBPattern);

    public static boolean matchEnd(String s){
        return endPattern.matcher(s).matches();
    }

    public static boolean matchStart(String s){
        return startPattern.matcher(s).matches();
    }

    public static String getDB(String sql){
        String[] strArr=sql.split("(\\s+|;)");
        if(strArr.length>1){
            return strArr[1];
        }else{
            return null;
        }
    }

    public static boolean matchUseDB(String s){
        return useDBPattern.matcher(s).matches();
    }

    public static boolean matchConnect(String s){
        return connPattern.matcher(s).matches();
    }

    public static boolean matchInitDB(String s){
        return initDBPattern.matcher(s).matches();
    }


    public static boolean matchInsertIgnore(String s){
        return insertIgnorePattern.matcher(s).matches();
    }
}
