package com.aliyun.gts.sniffer.common.utils;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Util {
    //解析Oracle sql，生成模板sqlId，如果解析失败，那么默认使用sqla生成的sqlId
    public static String toOracleSqlId(String sql,String preSqlId){
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.ORACLE;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){
            return preSqlId;
        }
    }

    //解析Oracle sql，生成模板sqlId，如果解析失败，那么默认使用sqla生成的sqlId
    public static String toOracleSqlId(String sql){
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.ORACLE;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
//                String fs=parameterize(sql).toString();
//                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){

        }
        return HexUtil.to16MD5(sql);
    }

    //解析Oracle sql，生成模板sqlId，如果解析失败，那么默认使用sqla生成的sqlId
    public static String toPolarDBOSqlId(String sql){
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.POLARDB;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return HexUtil.to16MD5(sql);
    }

    //返回sql参数化后的唯一标识
    public static String toPolarXSqlId(String sql) {
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.MYSQL;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception ignored){

        }
        return HexUtil.to16MD5(sql);

    }

    //返回sql参数化后的唯一标识
    public static String toPostgresqlSqlId(String sql) {
        try{
            if(sql.equals("BEGIN") || sql.equals("COMMIT")||sql.equals("ROLLBACK")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.POSTGRESQL;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception ignored){

        }
        return HexUtil.to16MD5(sql);

    }

    /**
     * 清理掉sql头部的hint
     * @param sql
     * @return
     */
    public static String trimHeaderHint(String sql){
        char x=' ';
        int i=0;
        for(;i<sql.length();i++){
            x=sql.charAt(i);
            if(x=='\t' ||x==' '){
                continue;
            }
            break;
        }
        if(x=='/'){
            i++;
            for(;i<sql.length();i++){
                x=sql.charAt(i);
                if(x=='/'){
                    break;
                }
            }
            //如果找不到hint的结尾，那么sql语法不对，返回整条sql
            if(i>=sql.length()){
                return sql;
            }
            return sql.substring(i+1).trim();
        }else{
            return sql;
        }

    }

    /**
     * *
     * 对SQL进行去参数化
     */
    public static StringBuilder parameterize(String sql){
        char c;
        boolean quoated=false;
        //boolean annotated=false;
        StringBuilder sb=new StringBuilder();
        int i=0;
        while(i<sql.length()){
            c=sql.charAt(i);
            if(!quoated){
                if(c=='\''){
                    sb.append('?');
                    quoated=true;
                    i++;
                }else{
                    sb.append(c);
                    i++;
                }
            }else{
                //sb.append(c);
                if(c=='\''){
                    int j=i+1;
                    //说明到最后一位。
                    if(j>=sql.length()) {
                        quoated=false;
                    }else{
                        char t=sql.charAt(j);
                        // '''' 场景
                        if(t!='\''){
                            quoated=false;
                            sb.append(' ');
                            sb.append(t);
                        }else{
                            quoated=true;
                        }
                        //sb.append(t);
                    }
                    i=j;
                    i++;
                }else{
                    i++;
                }
            }
        }
        return sb;
    }


}
