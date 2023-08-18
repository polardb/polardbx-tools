
package com.aliyun.gts.sniffer.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileUtil{
    private static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static File create(String f_path){
        File file = new File(f_path);
        try{
            if (!file.exists()){
                file.createNewFile();
            }
        }catch (IOException e) {
            logger.error("file create failed",e);
        }
        return file;
    }


}

