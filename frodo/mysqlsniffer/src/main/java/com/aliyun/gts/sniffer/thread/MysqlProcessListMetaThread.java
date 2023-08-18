package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.common.utils.JDBCUtils;
import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlProcessListMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;


public class MysqlProcessListMetaThread extends Thread{
    Logger logger= LoggerFactory.getLogger(MysqlProcessListMetaThread.class);
    private volatile boolean isRunning=true;
    private MysqlProcessListMeta meta;
    private JDBCUtils sourceJdbcUtils;

    public MysqlProcessListMetaThread(MysqlProcessListMeta meta) throws SQLException{
        this.meta=meta;
        String url="jdbc:mysql://"+ Config.getIp()+":"+ Config.getPort()+"/mysql?allowPublicKeyRetrieval=true";
        sourceJdbcUtils=new JDBCUtils(url,Config.getUsername(),Config.getPassword(),"mysql");
    }

    @Override
    public void run() {
        while(isRunning){
            //todo:每5秒更新一遍MysqlProcessListMetadata
            meta.setProcessList(sourceJdbcUtils.getProcessList());
            try{
                Thread.sleep(3000);
            }catch (InterruptedException e){
                isRunning=false;
            }

        }

    }
    public void close(){
        this.isRunning=false;
    }


}
