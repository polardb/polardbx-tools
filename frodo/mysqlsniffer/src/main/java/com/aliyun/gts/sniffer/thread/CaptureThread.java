package com.aliyun.gts.sniffer.thread;

import com.aliyun.gts.sniffer.core.Config;
import com.aliyun.gts.sniffer.mypcap.MysqlPacketHandler;
import com.aliyun.gts.sniffer.mypcap.MysqlPacketHandlerDeleted;
import jpcap.JpcapCaptor;
import jpcap.NetworkInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaoke on 17/10/30.
 */
public class CaptureThread extends AbstractCaptureThread{
    private Logger logger= LoggerFactory.getLogger(CaptureThread.class);
    private volatile boolean isRunning=true;
    private MysqlPacketHandler mysqlPacketHandler;
    private  NetworkInterface device=null;
    private JpcapCaptor jpcap;
    public JpcapCaptor getJpcap() {
        return jpcap;
    }

    public CaptureThread(MysqlPacketHandler mysqlPacketHandler, NetworkInterface device){
        this.mysqlPacketHandler=mysqlPacketHandler;
        this.device=device;
        int caplen = 64 * 1024;
        try {
            jpcap = JpcapCaptor.openDevice(device, caplen, true, 3000);
//            jpcap.setFilter("(tcp dst port "+ Config.getPort()+") or tcp src port"+Config.getPort()+ "( less "+Config.maxCaptureOutLength,true);
//            jpcap.setFilter("(tcp dst port "+ Config.getPort()+" and less 1000000 ) or ( tcp src port "+Config.getPort()+ " and less "+Config.maxCaptureOutLength+" )",true);
            jpcap.setFilter("(tcp dst port "+ Config.getPort()+" and less 10000000 ) or ( tcp src port "+Config.getPort()+ " and less "+Config.maxCaptureOutLength+" )",true);
            //            jpcap.setFilter("port 3306",true);
        }catch (Exception e){
            logger.error("抓包线程初始化失败",e);
            throw  new RuntimeException(e);
        }
    }


    public void run() {
        while(isRunning){
            try{
                jpcap.loopPacket(1000, mysqlPacketHandler);
            }catch (Exception e){
                logger.error("抓包出错",e);
                return;
            }
        }
        jpcap.close();
        jpcap=null;
    }

    public void close(){
        jpcap.breakLoop();
        isRunning=false;
    }

}
