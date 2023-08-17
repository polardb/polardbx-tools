package com.aliyun.gts.sniffer.mypcap;


import com.aliyun.gts.sniffer.common.entity.ProcessModel;
import com.aliyun.gts.sniffer.core.Config;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaoke on 17/10/28.
 */
public class MysqlProcessListMeta {
    private int maxProcesslistCacheSize=655350;

    private ConcurrentLinkedHashMap<String, ProcessModel> processList=new ConcurrentLinkedHashMap.Builder<String, ProcessModel>()
            .maximumWeightedCapacity(Config.maxProcesslistCacheSize)
            .weigher(Weighers.singleton())
            .build();
    private ConcurrentLinkedHashMap<String, ProcessModel> glProcessList=new ConcurrentLinkedHashMap.Builder<String, ProcessModel>()
            .maximumWeightedCapacity(Config.maxProcesslistCacheSize)
            .weigher(Weighers.singleton())
            .build();
    private static MysqlProcessListMeta mysqlProcessListMeta=null;
    private MysqlProcessListMeta(){}
    private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
//    private Lock r = rwl.readLock();
//    private Lock w = rwl.writeLock();
    private static Object x=new Object();

    public static synchronized MysqlProcessListMeta getInstance(){
        if(mysqlProcessListMeta==null){
            synchronized (x){
                mysqlProcessListMeta=new MysqlProcessListMeta();
            }

        }
        return mysqlProcessListMeta;
    }

    public void updateHost(String host,int srcPort,String username,String db){
        ProcessModel model=new ProcessModel();
        model.setDB(db);
        model.setUser(username);
        String key=host+":"+srcPort;
        model.setHost(key);
//        w.lock();
        if(processList.containsKey(key)){
            processList.replace(key,model);
        }else{
            processList.put(key,model);
        }

//        w.unlock();
    }

    //update db info
    public void updateGLModel(ProcessModel processModel){
        String key=processModel.getId()+"";
        ProcessModel model=glProcessList.get(key);
        if(model==null){
            glProcessList.put(key,processModel);
        }else{
            model.setDB(processModel.getDB());
        }



    }

    public ConcurrentLinkedHashMap<String, ProcessModel> getProcessList() {
        return processList;
    }

    public ConcurrentLinkedHashMap<String, ProcessModel> getGLProcessList() {
        return glProcessList;
    }

    public void setGLProcessList(HashMap<String, ProcessModel> process) {
//        w.lock();
        this.glProcessList.putAll(process);
//        w.unlock();
//        if(glProcessList.size()>maxProcesslistSize){
//            int rmCount=glProcessList.size()-maxProcesslistSize;
//            Set<String> set=glProcessList.keySet();
//            Iterator<String> iterator=set.iterator();
//            while(rmCount>0){
//                if(iterator.hasNext()){
//                    glProcessList.remove(iterator.next());
//                    rmCount--;
//                }else{
//                    break;
//                }
//            }
//
//        }

    }

    public void setProcessList(HashMap<String, ProcessModel> process) {
//        w.lock();
        this.processList.putAll(process);
//        w.unlock();
//        if(this.processList.size()>maxProcesslistSize){
//            int rmCount=this.processList.size()-maxProcesslistSize;
//            Set<String> set=this.processList.keySet();
//            Iterator<String> iterator=set.iterator();
//            while(rmCount>0){
//                if(iterator.hasNext()){
//                    this.processList.remove(iterator.next());
//                    rmCount--;
//                }else{
//                    break;
//                }
//            }
//        }
    }
    public boolean exists(String ip, int port){
        return processList.containsKey(ip+":"+port);
    }

    public ProcessModel getGLProcessModelByHost(String id){
//        r.lock();
        ProcessModel processModel=glProcessList.get(id);
//        r.unlock();
        return processModel;
    }

    public ProcessModel getProcessModelByHost(String ip, int port){
//        r.lock();
        ProcessModel processModel=processList.get(ip+":"+port);
//        r.unlock();
        return processModel;
    }

    /**
     * 获取连接当前连接的DB Name,如果元数据中没有该连接,那么直接返回null
     * 如果DB 为NULL,那么直接返回空字符串.
     */
    public String getDbNameByHost(String ip,int port){
        ProcessModel processModel=getProcessModelByHost(ip,port);
        if(processModel==null){
            return null;
        }else{
            return processModel.getDB()==null?"":processModel.getDB();
        }
    }



}
