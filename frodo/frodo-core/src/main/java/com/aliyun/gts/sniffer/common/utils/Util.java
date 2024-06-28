package com.aliyun.gts.sniffer.common.utils;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.sql.visitor.functions.Char;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Util {


    private static Set<Character> seperatorChar=new HashSet<>();

    static{
        seperatorChar.add(' ');
        seperatorChar.add('\n');
        seperatorChar.add('\t');
        seperatorChar.add(',');
        seperatorChar.add('(');
        seperatorChar.add(')');
        seperatorChar.add('=');
        seperatorChar.add('>');
        seperatorChar.add('<');
        seperatorChar.add('!');
        seperatorChar.add('~');// pg 风格的正则匹配
        seperatorChar.add('+');
        seperatorChar.add('-');
        seperatorChar.add('*');
        seperatorChar.add('/');
        seperatorChar.add('%');
        seperatorChar.add('^');
        seperatorChar.add(';');
    }


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
            if(sql.equals("begin") || sql.equals("commit")||sql.equals("rollback")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.ORACLE;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){
            //如果解析失败，那么使用通用的解析方法
            return toSqlId(sql);
        }
    }

    //解析Oracle sql，生成模板sqlId，如果解析失败，那么默认使用sqla生成的sqlId
    public static String toPolarDBOSqlId(String sql){
        try{
            if(sql.equals("begin") || sql.equals("commit")||sql.equals("rollback")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.POLARDB;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception e){
            //如果解析失败，那么使用通用的解析方法
            return toSqlId(sql);
        }
    }

    //返回sql参数化后的唯一标识
    public static String toPolarXSqlId(String sql) {
        try{
            if(sql.equals("begin") || sql.equals("commit")||sql.equals("rollback")){
                return HexUtil.to16MD5(sql);
            }else{
                DbType dbtype = JdbcConstants.MYSQL;
                String fs = ParameterizedOutputVisitorUtils.parameterize(sql,dbtype);
                return HexUtil.to16MD5(fs);
            }
        }catch (Exception ignored){
            //如果解析失败，那么使用通用的解析方法
            return toSqlId(sql);
        }

    }

    public static String toSqlId(String sql){
        try{
            // deparameterize 只做语法解析，不做语义解析，所以无法识别+ - 是正负号还是加减，直接替换成空格,减少同一个模板sql返回多个sqlid的概率
            StringBuilder fs = deparameterize(sql.replaceAll("[\\+\\-]"," "));
            return HexUtil.to16MD5(fs.toString());
        }catch (Exception ignored){

        }
        return HexUtil.to16MD5(sql);
    }

    //返回sql参数化后的唯一标识
    public static String toPostgresqlSqlId(String sql) {
        try{
            if(sql.equals("begin") || sql.equals("commit")||sql.equals("rollback")){
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
        boolean quoated=false;
        boolean multiLineCommented =false;
        for(;i<sql.length();i++){
            x=sql.charAt(i);
            if(x=='\t' ||x==' '){
                continue;
            }
            break;
        }
        //如果x 以任何空白字符或者'/' 开头，那么需要检查是否是注释hint
        while(Character.isWhitespace(x) ||Character.isSpaceChar(x) || x=='/'){
            if(x=='/'){
                i++;
                if(i>=sql.length()){
                    return sql;
                }
                x=sql.charAt(i);
                if(x=='*'){
                    multiLineCommented=true;
                }else{
                    return sql;
                }
                i++;

                for(;i<sql.length();i++){
                    x=sql.charAt(i);
                    //判断是否是单引号开头
                    if(quoated==false && x=='\''){
                        quoated=true;
                        i++;
                        //如果找不到注释的结尾，那么sql语法不对，返回整条sql
                        if(i>=sql.length()){
                            return sql;
                        }
                    }
                    //如果是字符串，那么找到字符串的结尾
                    if(quoated){
                        for(;i<sql.length();i++){
                            x=sql.charAt(i);
                            //如果出现单引号，判断紧跟着是否有单引号
                            if(x=='\''){
                                i++;
                                if(i>=sql.length()){
                                    break;
                                }
                                x=sql.charAt(i);
                                //说明是转义单引号,跳过继续
                                if(x=='\''){
                                    continue;
                                }else{
                                    quoated=false;
                                    break;
                                }
                            }
                        }
                    }
                    //如果找不到注释的结尾，那么sql语法不对，返回整条sql
                    if(i>=sql.length()){
                        return sql;
                    }
                    x=sql.charAt(i);
                    if(x=='*'){
                        i++;
                        //如果找不到注释的结尾，那么sql语法不对，返回整条sql
                        if(i>=sql.length()){
                            return sql;
                        }
                        x=sql.charAt(i);
                        if(x=='/'){
                            multiLineCommented=false;
                            break;
                        }
                    }
                }
                //如果找不到注释的结尾，那么sql语法不对，返回整条sql
                if(i>=sql.length()){
                    return sql;
                }
                i++;
                x=sql.charAt(i);
            }else{
                i++;
                x=sql.charAt(i);
            }
        }
        return sql.substring(i).trim();
    }


    public static String replaceHint(String sql){
        char c;
        boolean quoated=false;
        boolean oneLineCommented=false;
        boolean multiLineCommented =false;
        //boolean annotated=false;
        StringBuilder sb=new StringBuilder();
        int i=0;
        while(i<sql.length()){
            c=sql.charAt(i);
            if((!quoated)&&(!oneLineCommented)&&(!multiLineCommented)){
                if(c=='\''){
                    sb.append(c);
                    quoated=true;
                    i++;
                }else if(c=='-'){
                    sb.append(c);
                    int j=i+1;
                    //结束
                    if(j>=sql.length()){
                        i=j;
                        break;
                    }
                    char afterC=sql.charAt(j);
                    if(afterC=='-') {
                        oneLineCommented = true;
                        sb.append(afterC);
                        i=j;
                        i++;
                    } else{
                        sb.append(afterC);
                        i=j;
                        i++;
                    }

                }else if(c=='/'){
                    int j=i+1;
                    //结束
                    if(j>=sql.length()){
                        sb.append(c);
                        i=j;
                        break;
                    }
                    char afterC=sql.charAt(j);
                    if(afterC=='*'){
                        multiLineCommented =true;
                        //避免删除多行注释删除后，没有空格，导致语法报错
                        sb.append(' ');
                        i=j;
                        i++;
                    }else{
                        sb.append(afterC);
                        i=j;
                        i++;
                    }

                }else {
                    sb.append(c);
                    i++;
                }
            }else{
                while(quoated){
                    if(i>=sql.length()) {
                        quoated=false;
                        break;
                    }
                    c=sql.charAt(i);
                    sb.append(c);
                    if(c=='\''){
                        int j=i+1;
                        //说明到最后一位。
                        if(j>=sql.length()) {
                            quoated=false;
                            i=j;
                            break;
                        }else{
                            char t=sql.charAt(j);
                            // '''' 场景
                            if(t!='\''){
                                quoated=false;
                                i=j;
                            }else{
                                sb.append(t);
                                i=j;
                                i++;
                            }
                        }

                    }else{
                        i++;
                    }
                }

                while(oneLineCommented){
                    if(i>=sql.length()) {
                        oneLineCommented=false;
                        break;
                    }
                    c=sql.charAt(i);
                    sb.append(c);
                    if(c=='\n'){
                        oneLineCommented=false;
                        i++;
                    }else{
                        i++;
                    }
                }

                while(multiLineCommented){
                    if(i>=sql.length()) {
                        multiLineCommented=false;
                        break;
                    }
                    c=sql.charAt(i);
//                    sb.append(c);
                    if(c=='*'){
                        int j=i+1;
                        //说明到最后一位。
                        if(j>=sql.length()) {
                            multiLineCommented =false;
                            i=j;
                            break;
                        }else{
                            char t=sql.charAt(j);
                            // /*  */ 场景
                            if(t=='/'){
                                multiLineCommented =false;
                                i=j;
                                i++;
                            }else{
                                i++;
                            }
                        }
                    }else{
                        i++;
                    }
                }

            }
        }

        return sb.toString();
    }

    /**
     * *
     * 对SQL进行去参数化，对于带正负号的整数无法去参数化，会保留正负号
     */
    public static StringBuilder deparameterize(String sql){

//        Set<String> constStr=new HashSet<>();
//        constStr.add("true");
//        constStr.add("false");

        StringBuilder constToken;
        char c;
        boolean quoated=false;//判断是否是字符串常量
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
                    if(!seperatorChar.contains(c)){
                        constToken=new StringBuilder();
                        constToken.append(c);
                        i++;
                        while(true){
                            if(i>=sql.length()){
                                if(MysqlGLUtil.matchNumber(constToken.toString())){
                                    sb.append('?');
                                }else{
                                    sb.append(constToken);
                                }
                                break;
                            }
                            char tc=sql.charAt(i);
                            if(seperatorChar.contains(tc)){
                                if(MysqlGLUtil.matchNumber(constToken.toString())){
                                    sb.append('?');
                                }else{
                                    sb.append(constToken);
                                }
                                break;
                            }else{
                                constToken.append(tc);
                            }
                            i++;
                        }
                    }else{
                        sb.append(c);
                        i++;
                    }
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
//                            sb.append(' ');
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

    //对文本的双引号进行转义
    public static String escapeBy2Quote(String sql){
        char c;
        StringBuilder sb=new StringBuilder();
        int i=0;
        while(i<sql.length()){
            c=sql.charAt(i);
            if(c=='"'){
                sb.append(c);
                sb.append('"');
            }else{
                sb.append(c);
            }
            i++;
        }
        return sb.toString();
    }
}
