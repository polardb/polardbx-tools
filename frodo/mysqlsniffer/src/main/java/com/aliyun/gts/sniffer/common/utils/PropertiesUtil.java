package com.aliyun.gts.sniffer.common.utils;


import com.alibaba.druid.util.StringUtils;
import com.aliyun.gts.sniffer.core.Config;

import java.io.*;
import java.util.Objects;
import java.util.Properties;

public class PropertiesUtil {
    private static Properties _prop = new Properties();


//    public static void getPropertyValues(String pro) throws IOException {
//        File directory = new File("");
//        String courseFile = directory.getCanonicalPath();
//        String propertyFile = courseFile + File.separator + pro;
//        System.out.println(propertyFile);
//        PropertiesUtil.readProperties(propertyFile);
//    }

    /**
     * 读取配置文件
     *
     * @param fileName
     */
    public static void readProperties(String fileName) {
        try {
            File f = new File(fileName);
            InputStream in = new FileInputStream(f);
            //InputStream in = PropertiesUtil.class.getResourceAsStream("/"+fileName);
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            _prop.load(bf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据key读取对应的value
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return _prop.getProperty(key);
    }

    public static Integer getInteger(String key) {
       String value = getProperty(key);
       if(!StringUtils.isEmpty(value)){
           return  Integer.parseInt(value);
       }
       return null;
    }
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        if(!StringUtils.isEmpty(value)){
            return  Boolean.valueOf(value);
        }
        return null;
    }
    public static String getString(String key) {
        return getProperty(key);
    }

    public static void loadProperty(String path){
        readProperties(path);
        if(getInteger("maxPrepareStmtCacheSize")!=null){
            Config.maxPrepareStmtCacheSize=getInteger("maxPrepareStmtCacheSize");
        }
        if(getInteger("maxPacketQueueSize")!=null){
            Config.maxPacketQueueSize=getInteger("maxPacketQueueSize");
        }
        if(getInteger("maxLargePacketQueueSize")!=null){
            Config.maxLargePacketQueueSize=getInteger("maxLargePacketQueueSize");
        }
        if(getBoolean("stringToHex")!=null){
            Config.stringToHex=getBoolean("stringToHex");
        }
        if(getBoolean("initPSInfo")!=null){
            Config.initPSInfo=getBoolean("initPSInfo");
        }
        if(getString("glEndPattern")!=null){
            Config.glEndPattern=getString("glEndPattern");
        }
        if(getString("glStartPattern")!=null){
            Config.glStartPattern=getString("glStartPattern");
        }

        if(getString("glConnectPattern")!=null){
            Config.glConnectPattern=getString("glConnectPattern");
        }
        if(getString("glInitDBPattern")!=null){
            Config.glInitDBPattern=getString("glInitDBPattern");
        }

        if(getInteger("maxProcesslistCacheSize")!=null){
            Config.maxProcesslistCacheSize=getInteger("maxProcesslistCacheSize");
        }

        if(getInteger("maxGLTailCacheSize")!=null){
            Config.maxGLTailCacheSize=getInteger("maxGLTailCacheSize");
        }

        if(getInteger("maxCaptureOutLength")!=null){
            Config.maxCaptureOutLength=getInteger("maxCaptureOutLength");
        }
    }
}
