package com.aliyun.gts.slssniffer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    public static String  toChar(Date date,String format){
        if(date==null){
            return "";
        }
        SimpleDateFormat f = new SimpleDateFormat(format);
        return f.format(date);
    }
    public static String toChar(Date date){
        return toChar(date,"yyyy-MM-dd HH:mm:ss");
    }

    public static Date toDate(String dateStr,String format) throws ParseException{
        SimpleDateFormat f = new SimpleDateFormat(format);
        return f.parse(dateStr);
    }

    public static Date toDate(String dateStr) throws ParseException{
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return f.parse(dateStr);
    }

    public static String toChar(Long ts){
        Date date=new Date(ts);
        return toChar(date,"yyyy-MM-dd HH:mm:ss.SSS");
    }

//    public static String toChar(Long millSeconds){
//        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        return f.format(millSeconds);
//
//    }
}
